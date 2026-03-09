from __future__ import annotations
import base64
import hashlib
import hmac
import json
import os
import posixpath
import re
import secrets
import time
import urllib.parse
import zipfile
import io
import uvicorn

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

from fastmcp import FastMCP
from fastmcp.server.http import create_streamable_http_app

# Annotated + Field are used to enrich the JSON schema that the LLM sees for each tool.
# This massively improves tool selection and parameter correctness in OpenWebUI.
from typing import Annotated, Any
from pydantic import Field
from starlette.responses import JSONResponse, Response
from starlette.requests import Request

import mcp_backend_filters as filters_backend
import mcp_backend_products as products_backend
import mcp_backend_productdetails as details_backend
import mcp_backend_articles as articles_backend
import mcp_backend_visuals as visuals_backend

import mcp_backend_families as families_backend
import mcp_backend_system as system_backend
import mcp_backend_zipresolver as zipresolver_backend
from mcp_delivery_routes import register_delivery_routes
import mcp_backend_linkmanager as linkmanager_backend
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8102"))

mcp = FastMCP("Products MCP v1")

MCP_PUBLIC_URL = os.getenv("MCP_URL", os.getenv("PUBLIC_BASE_URL", "")).rstrip("/")
DOWNLOAD_URL_SECRET = os.getenv("MCP_DOWNLOAD_URL_SECRET", "change-me")
DOWNLOAD_URL_TTL_SECONDS = int(os.getenv("MCP_DOWNLOAD_URL_TTL_SECONDS", "900"))
DOWNLOAD_ROUTE_PATH = os.getenv("MCP_DOWNLOAD_ROUTE_PATH", "/mcp/downloads/articles").rstrip("/")
IMAGE_URL_SECRET = os.getenv("MCP_IMAGE_URL_SECRET", DOWNLOAD_URL_SECRET)
IMAGE_URL_TTL_SECONDS = int(os.getenv("MCP_IMAGE_URL_TTL_SECONDS", str(DOWNLOAD_URL_TTL_SECONDS)))
IMAGE_ROUTE_PATH = os.getenv("MCP_IMAGE_ROUTE_PATH", "/mcpvisuals/image").rstrip("/")
ZIP_URL_SECRET = os.getenv("MCP_ZIP_URL_SECRET", DOWNLOAD_URL_SECRET)
ZIP_URL_TTL_SECONDS = int(os.getenv("MCP_ZIP_URL_TTL_SECONDS", str(DOWNLOAD_URL_TTL_SECONDS)))
ZIP_ROUTE_PATH = os.getenv("MCP_ZIP_ROUTE_PATH", "/mcp/downloads/zips").rstrip("/")
ZIP_MAX_FILES = int(os.getenv("MCP_ZIP_MAX_FILES", "25"))
ZIP_MAX_TOTAL_BYTES = int(os.getenv("MCP_ZIP_MAX_TOTAL_BYTES", "104857600"))
ZIP_TOKEN_STORAGE_DIR = os.getenv("MCP_ZIP_TOKEN_STORAGE_DIR", "/tmp/prolicht_mcp_zip_tokens")
SHORT_LINK_ROUTE_PATH = os.getenv("MCP_SHORT_LINK_ROUTE_PATH", "/l").rstrip("/") or "/l"
SHORT_LINK_SQLITE_PATH = os.getenv("MCP_SHORT_LINK_SQLITE_PATH", "").strip() or None
SHORT_LINK_CODE_LENGTH = max(4, min(int(os.getenv("MCP_SHORT_LINK_CODE_LENGTH", "6")), 16))

linkmanager_backend.init_store(sqlite_path=SHORT_LINK_SQLITE_PATH)


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    pad = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + pad)


def _sign_payload(payload: dict, secret: str) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
    return f"{_b64url_encode(raw)}.{_b64url_encode(sig)}"


def _verify_signed_token(token: str, secret: str) -> dict | None:
    try:
        payload_part, sig_part = token.split(".", 1)
        raw = _b64url_decode(payload_part)
        expected_sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
        actual_sig = _b64url_decode(sig_part)
        if not hmac.compare_digest(expected_sig, actual_sig):
            return None
        payload = json.loads(raw.decode("utf-8"))
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        return payload
    except Exception:
        return None


def _sign_download_payload(payload: dict) -> str:
    return _sign_payload(payload, DOWNLOAD_URL_SECRET)


def _verify_download_token(token: str) -> dict | None:
    return _verify_signed_token(token, DOWNLOAD_URL_SECRET)


def _sign_image_payload(payload: dict) -> str:
    return _sign_payload(payload, IMAGE_URL_SECRET)


def _verify_image_token(token: str) -> dict | None:
    return _verify_signed_token(token, IMAGE_URL_SECRET)


def _build_public_download_url(token: str) -> str | None:
    if not MCP_PUBLIC_URL:
        return None
    return f"{MCP_PUBLIC_URL}{DOWNLOAD_ROUTE_PATH}/{token}"


def _build_public_image_url(token: str) -> str | None:
    if not MCP_PUBLIC_URL:
        return None
    return f"{MCP_PUBLIC_URL}{IMAGE_ROUTE_PATH}/{token}"

def _build_public_zip_url(token: str) -> str | None:
    if not MCP_PUBLIC_URL:
        return None
    return f"{MCP_PUBLIC_URL}{ZIP_ROUTE_PATH}/{token}"


def _build_short_public_link(public_url: str | None) -> tuple[str | None, str | None]:
    if not public_url or not MCP_PUBLIC_URL:
        return public_url, None
    code = linkmanager_backend.shorten_url(public_url, code_length=SHORT_LINK_CODE_LENGTH)
    short_route_path = f"{SHORT_LINK_ROUTE_PATH}/{code}"
    return f"{MCP_PUBLIC_URL}{short_route_path}", short_route_path




def _sign_zip_payload(payload: dict) -> str:
    return _sign_payload(payload, ZIP_URL_SECRET)


def _verify_zip_token(token: str) -> dict | None:
    return _verify_signed_token(token, ZIP_URL_SECRET)


def _ensure_zip_token_storage_dir() -> None:
    os.makedirs(ZIP_TOKEN_STORAGE_DIR, exist_ok=True)


def _zip_token_storage_path(short_token: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "", str(short_token or ""))
    return os.path.join(ZIP_TOKEN_STORAGE_DIR, f"{safe}.json")


def _store_zip_payload(payload: dict) -> str:
    _ensure_zip_token_storage_dir()
    short_token = secrets.token_urlsafe(12)
    path = _zip_token_storage_path(short_token)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
    os.replace(temp_path, path)
    return short_token


def _load_stored_zip_payload(short_token: str) -> dict | None:
    try:
        path = _zip_token_storage_path(short_token)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if int(payload.get("exp", 0)) < int(time.time()):
            try:
                os.remove(path)
            except OSError:
                pass
            return None
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _verify_zip_reference(token: str) -> dict | None:
    payload = _load_stored_zip_payload(token)
    if payload:
        return payload
    return _verify_zip_token(token)


def _normalize_zip_member_name(name: str, fallback: str) -> str:
    candidate = str(name or "").strip() or fallback
    candidate = candidate.replace("\\", "/").split("/")[-1]
    safe = ''.join(ch if ch not in '<>:"|?*' else '_' for ch in candidate).strip().strip('.')
    return safe or fallback


def _sanitize_folder_segment(value: str | None, fallback: str) -> str:
    raw = str(value or "").strip() or fallback
    raw = raw.replace("\\", "/").replace("/", " - ")
    safe = "".join(ch if ch not in '<>:"|?*' else "_" for ch in raw)
    safe = " ".join(safe.split()).strip().strip(".")
    return safe or fallback


def _extract_token_from_download_reference(reference: str) -> str | None:
    value = str(reference or "").strip()
    if not value:
        return None

    def _extract_path(raw_value: str) -> str | None:
        if raw_value.startswith("http://") or raw_value.startswith("https://"):
            try:
                parsed = urllib.parse.urlparse(raw_value)
                return parsed.path or ""
            except Exception:
                return None
        return raw_value

    path = _extract_path(value)
    if path is None:
        return None

    normalized_path = posixpath.normpath(path)
    short_prefix = f"{SHORT_LINK_ROUTE_PATH}/"
    if normalized_path.startswith(short_prefix):
        code = normalized_path[len(short_prefix):].strip("/")
        resolved_url = linkmanager_backend.resolve_short_code(code)
        if not resolved_url:
            return None
        path = _extract_path(str(resolved_url))
        if path is None:
            return None
        normalized_path = posixpath.normpath(path)

    prefix = f"{DOWNLOAD_ROUTE_PATH}/"
    if not normalized_path.startswith(prefix):
        return None
    token = normalized_path[len(prefix):].strip("/")
    return token or None


def _build_zip_download_token(
    files: list[dict],
    archive_name: str | None = None,
    expires_in_seconds: int | None = None,
) -> tuple[str, int]:
    ttl = int(expires_in_seconds or ZIP_URL_TTL_SECONDS)
    ttl = max(60, min(ttl, 86400))
    exp = int(time.time()) + ttl
    archive = str(archive_name or "downloads_bundle.zip").strip() or "downloads_bundle.zip"
    if not archive.lower().endswith(".zip"):
        archive = f"{archive}.zip"
    payload = {"files": files, "archive_name": archive, "exp": exp}
    short_token = _store_zip_payload(payload)
    return short_token, exp


def _resolve_zip_entries_from_download_references(references: list[str]) -> tuple[list[dict], list[dict]]:
    valid_entries: list[dict] = []
    rejected: list[dict] = []
    for reference in references:
        token = _extract_token_from_download_reference(reference)
        if not token:
            rejected.append({"reference": reference, "reason": "not_an_mcp_article_download_url"})
            continue
        payload = _verify_download_token(token)
        if not payload:
            rejected.append({"reference": reference, "reason": "invalid_or_expired_download_url"})
            continue
        sku = str(payload.get("sku") or "").strip()
        download_type = str(payload.get("download_type") or "").strip()
        language = str(payload.get("language") or "en").strip() or "en"
        workspace_id = payload.get("workspace_id")
        if not sku or not download_type:
            rejected.append({"reference": reference, "reason": "invalid_download_payload"})
            continue
        context = zipresolver_backend.get_zip_context_for_sku(sku=sku, language=language)
        valid_entries.append({
            "sku": sku,
            "download_type": download_type,
            "language": language,
            "workspace_id": workspace_id,
            "source_download_reference": reference,
            "zip_folder_path": context.get("folder_path"),
            "product_name": context.get("product_name"),
            "mounting_type": context.get("mounting_type"),
            "lighting_category": context.get("lighting_category"),
            "system_name": context.get("system_name"),
            "system_sku": context.get("system_sku"),
            "resolver_source": context.get("source"),
        })
    return valid_entries, rejected


def _build_article_download_token(
    sku: str,
    download_type: str,
    language: str = "en",
    workspace_id: str | None = None,
    expires_in_seconds: int | None = None,
) -> tuple[str, int]:
    ttl = int(expires_in_seconds or DOWNLOAD_URL_TTL_SECONDS)
    ttl = max(60, min(ttl, 86400))
    exp = int(time.time()) + ttl
    payload = {
        "sku": sku,
        "download_type": download_type,
        "language": language,
        "workspace_id": workspace_id,
        "exp": exp,
    }
    return _sign_download_payload(payload), exp


def _build_article_image_token(
    sku: str,
    language: str = "en",
    workspace_id: str | None = None,
    view: str | None = None,
    with_background: bool | None = None,
    width: int | None = None,
    height: int | None = None,
    allow_fallback: bool | None = None,
    mime_type: str | None = None,
    expires_in_seconds: int | None = None,
) -> tuple[str, int]:
    ttl = int(expires_in_seconds or IMAGE_URL_TTL_SECONDS)
    ttl = max(60, min(ttl, 86400))
    exp = int(time.time()) + ttl
    payload = {
        "sku": sku,
        "language": language,
        "workspace_id": workspace_id,
        "view": view,
        "with_background": with_background,
        "width": width,
        "height": height,
        "allow_fallback": allow_fallback,
        "mime_type": mime_type,
        "exp": exp,
    }
    return _sign_image_payload(payload), exp


def _build_article_image_link_payload(
    sku: str | None,
    language: str = "en",
    workspace_id: str | None = None,
    view: str | None = None,
    with_background: bool | None = None,
    width: int | None = None,
    height: int | None = None,
    allow_fallback: bool | None = None,
    mime_type: str | None = None,
    expires_in_seconds: int | None = None,
) -> dict | None:
    sku_value = str(sku or "").strip()
    if not sku_value:
        return None

    token, exp = _build_article_image_token(
        sku=sku_value,
        language=language,
        workspace_id=workspace_id,
        view=view,
        with_background=with_background,
        width=width,
        height=height,
        allow_fallback=allow_fallback,
        mime_type=mime_type,
        expires_in_seconds=expires_in_seconds,
    )
    image_url = _build_public_image_url(token)
    short_image_url, short_route_path = _build_short_public_link(image_url)
    effective_url = short_image_url or image_url
    route_path = short_route_path or f"{IMAGE_ROUTE_PATH}/{token}"
    return {
        "primary_article_sku": sku_value,
        "image_route_path": route_path,
        "image_url": effective_url,
        "markdown_image": f"![{sku_value}]({effective_url})" if effective_url else None,
        "markdown_link": f"[Open image]({effective_url})" if effective_url else None,
        "expires_at_unix": exp,
        "requested_mime_type": mime_type or "image/webp",
        "requested_view": view,
        "requested_width": width,
        "requested_height": height,
        "requested_with_background": with_background,
        "requested_allow_fallback": allow_fallback,
        "workspace_id": workspace_id,
        "language": language,
    }


def _attach_primary_image_links_to_product_list(
    results: list[dict],
    language: str,
    workspace_id: str | None = None,
) -> None:
    for result in results:
        if not isinstance(result, dict):
            continue
        primary = _build_article_image_link_payload(
            sku=result.get("primary_article_sku"),
            language=language,
            workspace_id=workspace_id,
            mime_type="image/webp",
        )
        if primary:
            result["primary_image"] = primary


def _image_extension_from_mime_type(value: str | None) -> str:
    mime = str(value or "").split(";", 1)[0].strip().lower()
    mapping = {
        "image/webp": "webp",
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/gif": "gif",
        "image/svg+xml": "svg",
    }
    return mapping.get(mime, "img")




KNOWN_DOWNLOAD_TYPES = {
    "tender_text",
    "assembly_instruction",
    "datasheet",
    "gldf",
    "3dobject_dwg",
    "3dobject_3dm",
    "3dobject_obj",
    "3dobject_sat",
    "3dobject_stp",
    "3dobject_fbx",
    "3dobject_rfa",
    "3dobject_dae",
    "3dobject_skp",
    "3dobject_l3d",
    "lightingdata_ldtzip",
    "lightingdata_eulumdat",
    "lightingdata_polarcurve_svg",
    "lightingdata_polarcurve_pdf",
    "lightingdata_ies",
    "relux_dwg",
    "relux_rfa",
    "relux_rolfz",
}


def _is_probable_download_type(value: Any) -> bool:
    s = str(value or "").strip().lower()
    if not s:
        return False
    if s in {"download", "downloads", "file", "files", "document"}:
        return False
    if s in KNOWN_DOWNLOAD_TYPES:
        return True
    return re.fullmatch(r"[a-z0-9]+(?:_[a-z0-9]+)*", s) is not None


def _normalize_download_type_candidate(value: Any) -> str | None:
    s = str(value or "").strip().lower()
    if not s:
        return None
    return s if _is_probable_download_type(s) else None


def _extract_download_type_from_node(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None

    preferred_keys = (
        "download_type",
        "downloadType",
        "download_key",
        "downloadKey",
        "key",
        "code",
        "slug",
    )
    for key in preferred_keys:
        cand = _normalize_download_type_candidate(node.get(key))
        if cand:
            return cand

    attrs = node.get("attributes")
    if isinstance(attrs, dict):
        for key in preferred_keys + ("name",):
            cand = _normalize_download_type_candidate(attrs.get(key))
            if cand:
                return cand

    name_cand = _normalize_download_type_candidate(node.get("name"))
    if name_cand:
        return name_cand

    node_id = str(node.get("id") or "").strip().lower()
    if _is_probable_download_type(node_id):
        m = re.match(r"^[a-z0-9.-]+_(.+)$", node_id)
        if m:
            tail = m.group(1).strip().lower()
            if _is_probable_download_type(tail):
                return tail
        return node_id

    if node_id:
        m = re.match(r"^[a-z0-9.-]+_(.+)$", node_id)
        if m:
            tail = m.group(1).strip().lower()
            if _is_probable_download_type(tail):
                return tail

    return None


def _build_short_download_link_payload(
    *,
    sku: str,
    download_type: str,
    language: str = "en",
    workspace_id: str | None = None,
    expires_in_seconds: int | None = None,
    label: str | None = None,
) -> dict:
    token, exp = _build_article_download_token(
        sku=sku,
        download_type=download_type,
        language=language,
        workspace_id=workspace_id,
        expires_in_seconds=expires_in_seconds,
    )
    long_route_path = f"{DOWNLOAD_ROUTE_PATH}/{token}"
    long_url = _build_public_download_url(token)
    short_url, short_route_path = _build_short_public_link(long_url)
    effective_url = short_url or long_url
    effective_route_path = short_route_path or long_route_path
    visible_label = str(label or f"{sku}_{download_type}").strip() or f"{sku}_{download_type}"
    return {
        "download_type": download_type,
        "expires_at_unix": exp,
        "download_route_path": effective_route_path,
        "download_url": effective_url,
        "markdown_link": f"[{visible_label}]({effective_url})" if effective_url else None,
    }


def _collect_download_types_from_listing(node: Any, found: set[str]) -> None:
    if isinstance(node, list):
        for item in node:
            _collect_download_types_from_listing(item, found)
        return

    if isinstance(node, dict):
        cand = _extract_download_type_from_node(node)
        if cand:
            found.add(cand)
        for value in node.values():
            _collect_download_types_from_listing(value, found)
        return

    cand = _normalize_download_type_candidate(node)
    if cand:
        found.add(cand)


def _attach_download_links_to_listing(
    node: Any,
    *,
    sku: str,
    language: str = "en",
    workspace_id: str | None = None,
    expires_in_seconds: int | None = None,
    links: list[dict] | None = None,
    seen_download_types: set[str] | None = None,
) -> None:
    if isinstance(node, list):
        for item in node:
            _attach_download_links_to_listing(
                item,
                sku=sku,
                language=language,
                workspace_id=workspace_id,
                expires_in_seconds=expires_in_seconds,
                links=links,
                seen_download_types=seen_download_types,
            )
        return

    if not isinstance(node, dict):
        return

    download_type = _extract_download_type_from_node(node)
    if download_type:
        label = (
            node.get("label")
            or node.get("title")
            or node.get("name")
            or (
                node.get("attributes", {}).get("label")
                if isinstance(node.get("attributes"), dict)
                else None
            )
            or (
                node.get("attributes", {}).get("title")
                if isinstance(node.get("attributes"), dict)
                else None
            )
            or (
                node.get("attributes", {}).get("name")
                if isinstance(node.get("attributes"), dict)
                else None
            )
            or download_type
        )
        payload = _build_short_download_link_payload(
            sku=sku,
            download_type=download_type,
            language=language,
            workspace_id=workspace_id,
            expires_in_seconds=expires_in_seconds,
            label=str(label),
        )
        node.update(payload)

        if links is not None:
            links.append(
                {
                    "download_type": download_type,
                    "label": str(label),
                    "download_url": payload.get("download_url"),
                    "download_route_path": payload.get("download_route_path"),
                    "markdown_link": payload.get("markdown_link"),
                    "expires_at_unix": payload.get("expires_at_unix"),
                }
            )
        if seen_download_types is not None:
            seen_download_types.add(download_type)

    for value in node.values():
        _attach_download_links_to_listing(
            value,
            sku=sku,
            language=language,
            workspace_id=workspace_id,
            expires_in_seconds=expires_in_seconds,
            links=links,
            seen_download_types=seen_download_types,
        )


# Gemeinsame Typ-Aliase für bessere JSON-Schemas
# NOTE FOR LLMs (important):
# - Use ONLY filter IDs and option keys returned by get_product_search_options.
# - Do NOT invent filter names (e.g. don't guess "recessed" vs "RECESSED").
# - Values must be the machine keys (values[].key), not the human label.
FilterDict = Annotated[
    dict[str, list[str]],
    Field(
        description=(
            "Product search filters. Keys MUST be valid filter IDs from get_product_search_options "
            "(e.g. 'product_family', 'product', 'mounting_type', 'product_categories', "
            "'segments', 'applications', 'color_temperatures', 'ip_rates', 'dimmability', ...). "
            "Values MUST be lists of allowed option keys (values[].key) from get_product_search_options.\n\n"
            "Rule of thumb: if the user mentions a filter in free text (e.g. \"Büro\", \"Office\", \"Workplace\", "
            "\"Einbau\", \"Anbau\", \"IP20\", \"3000K\"), call get_product_search_options() first to get "
            "the exact filter key + option key, then call search_products.\n\n"
            "Common DE mappings (use option keys from get_product_search_options):\n"
            "  Büro / Office -> {'segments': ['Office']}\n"
            "  Anwendung (z.B. Workplace) -> {'applications': ['Workplace']}\n"
            "  Einbau -> {'mounting_type': ['RECESSED']}\n"
            "  Anbau -> {'mounting_type': ['SURFACE_MOUNTED']}\n"
            "  Pendel -> {'mounting_type': ['PENDANT']}\n\n"
            "Examples:\n"
            "  {'segments': ['Office'], 'mounting_type': ['RECESSED']}\n"
            "  {'product_categories': ['DOWNLIGHT'], 'ip_rates': ['IP20']}\n"
            "  {'product_family': ['MAGIQ', 'INVADER']}\n"
            "  {'system_sku': ['8150-094']}  # fetch inserts/components by system_sku"
)
    ),
]

Language = Annotated[
    str,
    Field(
        description=(
            "Response language forwarded to the API (query param 'lang'). "
            "Use 'en' or 'de'. Default: 'en'."
        )
    ),
]

ActiveFilters = Annotated[
    dict[str, list[str]] | None,
    Field(
        description=(
            "Optional faceted narrowing: same structure as `filters` in search_products. "
            "If provided, the API returns filter options that remain valid within this selection.\n\n"
            "Use this to progressively narrow filters:\n"
            "  1) call get_product_search_options() to see all filters\n"
            "  2) set active_filters based on user intent\n"
            "  3) call get_product_search_options(active_filters=...) to get remaining valid options\n\n"
            "IMPORTANT: active_filters should use ONLY keys/values returned by get_product_search_options."
        )
    ),
]

Limit = Annotated[
    int,
    Field(
        ge=1,
        le=200,
        description="Page size for search_products (API page[limit]). Default: 50.",
    ),
]

ProductName = Annotated[
    str,
    Field(
        description=(
            "Product display name as seen in the product list (e.g. 'INVADER ADJUSTABLE'). "
            "This is not a SKU. The tool will resolve it to numeric_product_id + mounting_type + lighting_category."
        )
    ),
]









MountingTypeKey = Annotated[
    str | None,
    Field(
        description=(
            "Optional mounting type machine key used to disambiguate variants when a product name exists in multiple mountings. "
            "Use ONLY keys like 'RECESSED', 'SURFACE_MOUNTED', 'PENDANT', 'TRACK' (never human labels)."
        )
    ),
]

LightingCategoryKey = Annotated[
    str | None,
    Field(
        description=(
            "Optional lighting category machine key used to disambiguate variants when a product name exists in multiple categories. "
            "Use ONLY keys like 'DOWNLIGHT', 'SPOTLIGHT', 'WALL_WASHER', ... (never human labels)."
        )
    ),
]

NumericProductId = Annotated[
    int | None,
    Field(
        ge=1,
        description=(
            "Optional numeric_product_id from search_products results. "
            "Strongest disambiguator when product_name exists in multiple variants."
        ),
    ),
]

SystemSku = Annotated[
    str | None,
    Field(
        description=(
            "Optional system SKU context (e.g. '8150-094'). "
            "Required when fetching ITEM details for a system insert/component that belongs to a system. "
            "If provided, it is passed as query parameter `system_sku` to the ITEM endpoint."
        )
    ),
]


ArticleSku = Annotated[
    str,
    Field(
        description=(
            "Exact article SKU. Use a SKU returned by /products results or by article/system responses."
        )
    ),
]

WorkspaceId = Annotated[
    str | None,
    Field(
        description=(
            "Optional workspace_id for article download listing or file download endpoints."
        )
    ),
]

DownloadType = Annotated[
    str,
    Field(
        description=(
            "Download type path parameter for /articles/{sku}/downloads/{type}, for example a type returned by get_article_downloads."
        )
    ),
]

ExpiresInSeconds = Annotated[
    int,
    Field(
        ge=60,
        le=86400,
        description=(
            "Validity of the public download link in seconds. Default: 900 (15 minutes)."
        ),
    ),
]

ZipDownloadReferences = Annotated[
    list[str],
    Field(
        description=(
            "List of MCP article download URLs or route paths created by get_article_download_file. "
            "Use the returned download_url or download_route_path values from that tool. Do not pass arbitrary web links."
        ),
        min_length=1,
        max_length=25,
    ),
]

ZipFilename = Annotated[
    str | None,
    Field(description="Optional ZIP filename for the generated archive. '.zip' is added automatically if missing."),
]

VisualView = Annotated[
    str | None,
    Field(description="Optional visuals API view parameter, for example a specific camera angle."),
]

VisualMimeType = Annotated[
    str | None,
    Field(description="Optional visuals API MIME type, for example image/webp or image/png. Default: image/webp."),
]

VisualSize = Annotated[
    int | None,
    Field(ge=1, le=8192, description="Optional image width/height in pixels for visuals endpoint rendering."),
]

# -------------------------------------------------
# Tools
# -------------------------------------------------

@mcp.tool
def list_product_families(language: Language = "en"):
    """
    Returns ALL available product families.
    Use this before filtering if unsure about valid family keys.
    """
    raw = filters_backend.fetch_product_filters(lang=language)
    return filters_backend.extract_product_families(raw)


@mcp.tool
def get_product_search_options(active_filters: ActiveFilters = None, language: Language = "en"):
    """
    Search helper: returns VALID filter IDs and allowed values for product search.

    What you get:
    - For each filter (e.g. product_family, product, mounting_type, product_categories, segments, applications, ...),
      you receive allowed option keys (values[].key) that can be used in search_products.

    When to use:
    - BEFORE search_products if the user mentions filters in free text (e.g. "Büro/Office", "Workplace", "Einbau", "IP20", "3000K").
    - If you are unsure which filter IDs exist or which option keys are valid.

    How to use it:
    - Map user intent to filter IDs and option keys from this response (use ONLY values[].key).
    - Then call search_products(filters={...}) using ONLY those keys.

    Important:
    - If active_filters is omitted: returns ALL possible options.
    - If active_filters is provided: returns options remaining within that filtered result set (faceted filtering).

    LLM Guidance:
    - segments/applications are marketing filters (e.g. Büro -> segments=['Office']).
    - For questions like "welche SIGN gibt es in Anbau" or "Produkte fürs Büro":
      1) get_product_search_options(language='de')
      2) search_products(filters={'product_family':['SIGN'], 'mounting_type':['SURFACE_MOUNTED']}, language='de')
         or search_products(filters={'segments':['Office']}, language='de')

Important:
- This tool ONLY returns filter OPTIONS (valid keys and allowed values). It does NOT return products.
- To get an actual product list, ALWAYS follow with search_products(filters=...).

Two-step process for natural language queries:
1) Call get_product_search_options(language=...) to identify the correct filter key + value keys.
2) Call search_products(filters={...}, language=...) to retrieve matching products.

Example (Kindergarten):
- get_product_search_options(language="de")
- search_products(filters={"applications":["SchoolKindergardenUniversity"], "segments":["Education"]}, language="de")
"""
    raw = filters_backend.fetch_product_filters(active_filters, language)
    llm_usage = {
        "rule": "Use only option keys (values[].key) from `filters` as filter values in search_products. Do not invent keys or values.",
        "two_step_process": [
            "1) Call get_product_search_options(language=...) to get valid filter keys and values[].key.",
            "2) Call search_products(filters={...}) using ONLY those keys to retrieve products."
        ],
        "must_run_search_products_for_lists": "If the user asks for a product list (e.g. segment/application like Retail/Office/Kindergarten), you MUST call search_products and return results. Do not reply that only options exist.",
        "example_retail": {"filters": {"segments": ["Retail"]}},
        "example_kindergarten": {"filters": {"applications": ["SchoolKindergardenUniversity"], "segments": ["Education"]}},
        "de_synonyms": {
            "Büro": {"segments": ["Office"]},
            "Office": {"segments": ["Office"]},
            "Anbau": {"mounting_type": ["SURFACE_MOUNTED"]},
            "Einbau": {"mounting_type": ["RECESSED"]},
            "Pendel": {"mounting_type": ["PENDANT"]},
        },
        "common_recipes": [
            {"intent": "Produkte fürs Büro", "filters": {"segments": ["Office"]}},
            {"intent": "Büro + Einbau", "filters": {"segments": ["Office"], "mounting_type": ["RECESSED"]}},
            {"intent": "Produkte fürs Segment Retail", "filters": {"segments": ["Retail"]}},
            {"intent": "Kindergarten/Schule", "filters": {"applications": ["SchoolKindergardenUniversity"], "segments": ["Education"]}},
        ],
    }
    return {"llm_usage": llm_usage, "filters": raw}


@mcp.tool
def search_products(
    filters: FilterDict = Field(default_factory=dict),
    language: Language = "en",
    limit: Limit = 50,
    include_raw: Annotated[
        bool,
        Field(
            description=(
                "If true, returns both a compact LLM-friendly result AND the full raw API payload. "
                "Default is false to avoid huge outputs that can overwhelm the model."
            )
        ),
    ] = False,
):
    """
    Search products using filter keys from get_product_search_options.

    Returns:
    - By default (include_raw=False): a PRUNED list with the most important fields per product variant.
      This keeps responses small and reliable for LLMs.
    - If include_raw=True: includes the original API JSON in addition to the pruned list (debug only).

    LLM Guidance:
    - If you are unsure which filter keys/values are valid, call get_product_search_options first.
    - Do not guess filter keys or option keys.
    - Use get_product_details to fetch the ITEM endpoint for full technical details of one product.

Critical note (MUST follow):
- If the user asks for a PRODUCT LIST using marketing intent (segment/application) or any other filter
  (e.g. "alle Produkte fürs Segment Retail", "für Kindergarten", "für Office/Büro", "für Hotel", "Anwendung Workplace"),
  you MUST execute a product search and return the results from this tool.
- Do NOT answer with "no concrete product list available" if the API can be queried.
- Do NOT ask follow-up questions like mounting type unless the user explicitly wants a narrower subset.
  Default behavior: search with the given filters and return all matching products (then offer optional refinement).
- Two-step rule:
  1) If you are unsure about the correct filter key/value, call get_product_search_options(language=...) to confirm.
  2) Then call search_products(filters={...}).
- Shortcut:
  - If the user explicitly names an option key/value that is obviously valid (e.g. "Retail", "Office", "Education"),
    you may call search_products directly (still prefer keys from get_product_search_options when in doubt).


"""
    raw = products_backend.search_products(
        filters=filters,
        lang=language,
        limit=limit,
    )
    pruned = products_backend.prune_product_list(raw)
    pruned_results = pruned.get("results") or []
    _attach_primary_image_links_to_product_list(pruned_results, language=language)

    # Group variants by product_name so the chat layer can list available mountings/categories
    # without guessing or "mixing" variants across products.
    grouped_map: dict[str, dict[str, object]] = {}
    for r in pruned_results:
        name = (r.get("product_name") or "").strip()
        if not name:
            continue
        g = grouped_map.setdefault(
            name,
            {"product_name": name, "mounting_types": set(), "lighting_categories": set(), "variant_count": 0, "primary_image": r.get("primary_image")},
        )
        mt = (r.get("mounting_type") or "").strip()
        lc = (r.get("lighting_category") or "").strip()
        if mt:
            g["mounting_types"].add(mt)  # type: ignore[attr-defined]
        if lc:
            g["lighting_categories"].add(lc)  # type: ignore[attr-defined]
        if not g.get("primary_image") and r.get("primary_image"):
            g["primary_image"] = r.get("primary_image")
        g["variant_count"] = int(g.get("variant_count", 0)) + 1  # type: ignore[arg-type]

    grouped_by_product_name = []
    for name, g in sorted(grouped_map.items(), key=lambda kv: kv[0]):
        grouped_by_product_name.append(
            {
                "product_name": name,
                "variant_count": g["variant_count"],
                "mounting_types": sorted(list(g["mounting_types"])),  # type: ignore[arg-type]
                "lighting_categories": sorted(list(g["lighting_categories"])),  # type: ignore[arg-type]
                "primary_image": g.get("primary_image"),
            }
        )

    response = {
        "query": {"filters": filters, "language": language, "limit": limit},
        "meta": raw.get("meta", {}) if isinstance(raw, dict) else {},
        "results": pruned,
        "grouped_by_product_name": grouped_by_product_name,
    }

    if include_raw:
        response["raw"] = raw
    return response


@mcp.tool
def get_system_inserts(
    product_family_id: Annotated[
        str,
        Field(description="Product family key, e.g. 'HYPRO'."),
    ],
    product_name: Annotated[
        str,
        Field(description="System product display name (attributes.id), e.g. 'HYPRO 40'."),
    ],
    mounting_type: Annotated[
        str,
        Field(description="Mounting type machine key, e.g. 'RECESSED', 'PENDANT', 'SURFACE_MOUNTED'."),
    ],
    language: Language = "en",
    limit: Limit = 200,
):
    """Get inserts/components for a system variant WITHOUT requiring the user to provide a SKU.

    This tool resolves the system_sku for the requested system variant (family + product name + mounting type)
    and then calls /products with filter[system_sku]=... to list the inserts/components.

    Returns a pruned list with aggregated technical ranges per insert.
    """
    result = system_backend.get_system_inserts(
        product_family_id=product_family_id,
        product_name=product_name,
        mounting_type=mounting_type,
        language=language,
        limit=limit,
    )
    inserts = ((result.get("inserts") or {}).get("results") or []) if isinstance(result, dict) else []
    _attach_primary_image_links_to_product_list(inserts, language=language)
    return result

@mcp.tool
def get_products_grouped_by_family(
    family_ids: Annotated[
        list[str] | None,
        Field(
            description=(
                "Optional list of product family keys (e.g. ['MAGIQ','INVADER']). "
                "If omitted or null, the tool will fetch all families via /products/filters "
                "and then aggregate products for each family."
            )
        ),
    ] = None,
    language: Language = "en",
    per_page: Annotated[
        int,
        Field(
            ge=1,
            le=200,
            description=(
                "Internal page size for API pagination (page[limit]). "
                "Higher values reduce requests but increase payload size. Default: 50."
            ),
        ),
    ] = 50,
    max_variants_per_family: Annotated[
        int,
        Field(
            ge=1,
            le=5000,
            description=(
                "Safety limit: maximum number of product variants to return per family. "
                "Prevents overly large responses. Default: 500."
            ),
        ),
    ] = 500,
    max_total_variants: Annotated[
        int,
        Field(
            ge=1,
            le=20000,
            description=(
                "Safety limit: maximum number of product variants to return across ALL families. "
                "Prevents overly large responses. Default: 3000."
            ),
        ),
    ] = 3000,
    include_raw: Annotated[
        bool,
        Field(
            description=(
                "If true, also returns raw page payloads for debugging. "
                "Default false (raw payloads can be huge)."
            )
        ),
    ] = False,
):
    """
    Build a deterministic mapping: product_family -> product variants, using ONLY the WebAPI.

    Why this exists:
    - /products/filters returns only the family keys (filter options), not the contained products.
    - For 'family -> products' answers we must query /products and group by product_family_id.

    Output is compact/pruned per variant (to avoid 100k+ character payloads):
    - product_name, numeric_product_id, product_family_id
    - mounting_type (key), lighting_category (key)
    - is_system (if present)
    - contained_article_skus ONLY when is_system==true (needed to fetch inserts later)
    """
    return families_backend.get_products_grouped_by_family(
        family_ids=family_ids,
        language=language,
        per_page=per_page,
        max_variants_per_family=max_variants_per_family,
        max_total_variants=max_total_variants,
        include_raw=include_raw,
    )


@mcp.tool
def get_products_details_by_ids(
    product_ids: Annotated[
        list[str],
        Field(
            description=(
                "Advanced/debug: list of product IDs as used by the /products list endpoint "
                "(not the ITEM endpoint). Prefer get_product_details for end-user requests."
            )
        ),
    ],
    product_family: Annotated[
        str | None,
        Field(description="Optional product family key for narrowing (rarely needed)."),
    ] = None,
    language: Language = "en",
):
    """
    Advanced: fetch raw product details by explicit IDs (list endpoint).
    Note: This does NOT call the ITEM endpoint. For end-user technical details use get_product_details.
    """
    return details_backend.fetch_product_details_by_ids(
        product_ids=product_ids,
        product_family=product_family,
        lang=language,
    )



@mcp.tool
def resolve_product_variants(
    product_name: ProductName,
    language: Language = "en",
    system_sku: SystemSku = None,
    limit: Limit = 50,
    include_raw: Annotated[
        bool,
        Field(
            description=(
                "If true, also return the raw /products response for debugging. "
                "Default false."
            )
        ),
    ] = False,
):
    """Resolve all list-endpoint variants for a given product_name.

    Use this when the same product_name can exist in multiple mounting types / lighting categories
    and you need to confirm whether a specific variant (e.g. SURFACE_MOUNTED) exists *before*
    calling get_product_details().
    """

    base_filters: dict[str, list[str]] = {}
    if system_sku:
        base_filters["system_sku"] = [system_sku]

    filters_exact = dict(base_filters)
    filters_exact["product"] = [product_name]
    raw = details_backend.search_products_raw(filters=filters_exact, language=language, limit=limit)

    data = raw.get("data", []) if isinstance(raw, dict) else []
    if not data:
        filters_term = dict(base_filters)
        filters_term["term"] = [product_name]
        raw = details_backend.search_products_raw(filters=filters_term, language=language, limit=max(limit, 20))

    pruned = products_backend.prune_product_list(raw)
    pruned_results = pruned.get("results", []) or []

    # Prefer exact (case-insensitive) id matches if term search returned broader results.
    exact = [r for r in pruned_results if (r.get("product_name") or "").lower() == product_name.lower()]
    if exact:
        pruned_results = exact

    variants_map: dict[tuple, dict[str, object]] = {}
    for r in pruned_results:
        key = (
            r.get("numeric_product_id"),
            r.get("mounting_type"),
            r.get("lighting_category"),
            bool(r.get("is_system", False)),
            r.get("product_family_id"),
        )
        variants_map.setdefault(
            key,
            {
                "numeric_product_id": r.get("numeric_product_id"),
                "mounting_type": r.get("mounting_type"),
                "lighting_category": r.get("lighting_category"),
                "is_system": bool(r.get("is_system", False)),
                "product_family_id": r.get("product_family_id"),
            },
        )

    out = {
        "query": {"product_name": product_name, "language": language, "system_sku": system_sku, "limit": limit},
        "variants": list(variants_map.values()),
        "pruned": {"meta": pruned.get("meta"), "results": pruned_results},
    }
    if include_raw:
        out["raw"] = raw
    return out




@mcp.tool
def get_product_details(
    product_name: ProductName,
    numeric_product_id: NumericProductId = None,
    mounting_type: MountingTypeKey = None,
    lighting_category: LightingCategoryKey = None,
    language: Language = "en",
    system_sku: SystemSku = None,
):
    """Returns full technical details for a product (ITEM endpoint).

    This tool is VARIANT-AWARE:
    - If a product_name exists in multiple mounting types / lighting categories, you can (and should)
      pass mounting_type and/or lighting_category and/or numeric_product_id (from search_products)
      to fetch details for the intended variant.
    - If the selection is ambiguous, the tool returns a structured list of available variants
      instead of guessing.

    Flow:
      1) Search /products (LIST endpoint) by product name (and optional system_sku context)
      2) Select the correct variant (numeric_product_id + mounting_type + lighting_category)
      3) Call the ITEM endpoint:
         /products/{numeric_product_id}/{mounting_type}/{lighting_category}
         If system_sku is provided, it is passed as query param ?system_sku=... (required for system inserts).
      4) Return pruned technical details
    """

    def _norm_key(s: str | None) -> str | None:
        if s is None:
            return None
        ss = str(s).strip()
        return ss.upper() if ss else None

    def _as_int(x) -> int | None:
        try:
            return int(x)
        except Exception:
            return None

    base_filters: dict[str, list[str]] = {}
    if system_sku:
        base_filters["system_sku"] = [system_sku]

    # Prefer exact product filter first (when product_name is a valid option key),
    # otherwise fall back to fuzzy term search.
    filters_exact = dict(base_filters)
    filters_exact["product"] = [product_name]
    search_result = details_backend.search_products_raw(
        filters=filters_exact,
        language=language,
        limit=50,
    )

    data = search_result.get("data", []) if isinstance(search_result, dict) else []
    if not data:
        filters_term = dict(base_filters)
        filters_term["term"] = [product_name]
        search_result = details_backend.search_products_raw(
            filters=filters_term,
            language=language,
            limit=50,
        )
        data = search_result.get("data", []) if isinstance(search_result, dict) else []

    if not data:
        return {
            "error": f"No product found for '{product_name}'",
            "hint": "Call search_products with broader filters (or resolve_product_variants) to verify available product names/variants. If this is a system insert, pass system_sku.",
        }

    # Prefer exact match on attributes.id (case-insensitive) when term search returned broader results.
    exact_matches = []
    for item in data:
        attrs = item.get("attributes", {}) or {}
        if str(attrs.get("id") or "").strip().lower() == product_name.strip().lower():
            exact_matches.append(item)

    candidates = exact_matches or list(data)

    # Build availability list (for helpful errors / ambiguity).
    available_variants = []
    for item in candidates:
        attrs = item.get("attributes", {}) or {}
        available_variants.append(
            {
                "numeric_product_id": attrs.get("numeric_product_id"),
                "mounting_type": details_backend._key_or_value(attrs.get("mounting_type")),
                "lighting_category": details_backend._key_or_value(attrs.get("lighting_category")),
                "is_system": bool(attrs.get("is_system", False)),
                "product_family_id": attrs.get("product_family_id"),
            }
        )

    # Apply disambiguation filters (if provided).
    wanted_np = _as_int(numeric_product_id) if numeric_product_id is not None else None
    wanted_mt = _norm_key(mounting_type)
    wanted_lc = _norm_key(lighting_category)

    filtered = []
    for item in candidates:
        attrs = item.get("attributes", {}) or {}

        if wanted_np is not None:
            if _as_int(attrs.get("numeric_product_id")) != wanted_np:
                continue

        if wanted_mt is not None:
            mt_key = _norm_key(details_backend._key_or_value(attrs.get("mounting_type")))
            if mt_key != wanted_mt:
                continue

        if wanted_lc is not None:
            lc_key = _norm_key(details_backend._key_or_value(attrs.get("lighting_category")))
            if lc_key != wanted_lc:
                continue

        filtered.append(item)

    if not filtered:
        return {
            "error": "No matching variant found for the requested constraints.",
            "debug": {
                "product_name": product_name,
                "numeric_product_id": numeric_product_id,
                "mounting_type": mounting_type,
                "lighting_category": lighting_category,
                "system_sku": system_sku,
            },
            "available_variants": available_variants,
            "hint": "Use resolve_product_variants(product_name=...) or search_products(...) to find the correct numeric_product_id/mounting_type/lighting_category, then call get_product_details again with those values.",
        }

    if len(filtered) > 1:
        return {
            "error": "Ambiguous product selection: multiple variants match. Please provide more constraints.",
            "debug": {
                "product_name": product_name,
                "numeric_product_id": numeric_product_id,
                "mounting_type": mounting_type,
                "lighting_category": lighting_category,
                "system_sku": system_sku,
                "matches": len(filtered),
            },
            "available_variants": available_variants,
            "hint": "Provide numeric_product_id (preferred) and/or mounting_type and lighting_category to disambiguate.",
        }

    selected = filtered[0].get("attributes", {}) or {}

    numeric_id = selected.get("numeric_product_id")
    mt = selected.get("mounting_type")
    lc = selected.get("lighting_category")

    if not numeric_id:
        return {"error": "numeric_product_id missing in product response", "available_variants": available_variants}
    if not mt:
        return {"error": "mounting_type missing in product response", "available_variants": available_variants}
    if not lc:
        return {"error": "lighting_category missing in product response", "available_variants": available_variants}

    raw_details = details_backend.fetch_product_item(
        numeric_product_id=numeric_id,
        mounting_type=mt,
        lighting_category=lc,
        language=language,
        system_sku=system_sku,
    )

    pruned_details = details_backend.prune_details(raw_details)
    detail_items = pruned_details.get("data") or []
    _attach_primary_image_links_to_product_list(detail_items, language=language)
    return pruned_details


@mcp.tool
def get_article_details(
    sku: ArticleSku,
    language: Language = "en",
):
    """Get detailed article information for one exact SKU via /articles/{sku}."""
    return articles_backend.get_article_details(sku=sku, language=language)


@mcp.tool
def get_article_accessories(
    sku: ArticleSku,
    language: Language = "en",
    include_optional: Annotated[
        bool | None,
        Field(description="Include optional accessories when true."),
    ] = None,
):
    """Get accessories for one exact article SKU via /articles/{sku}/accessories."""
    return articles_backend.get_article_accessories(
        sku=sku,
        language=language,
        include_optional=include_optional,
    )


@mcp.tool
def get_article_visual_image(
    sku: ArticleSku,
    language: Language = "en",
    workspace_id: WorkspaceId = None,
    view: VisualView = None,
    with_background: Annotated[
        bool | None,
        Field(description="Optional visuals API flag to include background."),
    ] = None,
    width: VisualSize = None,
    height: VisualSize = None,
    allow_fallback: Annotated[
        bool | None,
        Field(description="Optional visuals API flag. If true, upstream may return an empty fallback image instead of an error."),
    ] = None,
    mime_type: VisualMimeType = "image/webp",
    expires_in_seconds: ExpiresInSeconds = 900,
):
    """Return a public, time-limited HTTPS image URL for one exact article SKU via /visuals/image/{sku}."""
    token, exp = _build_article_image_token(
        sku=sku,
        language=language,
        workspace_id=workspace_id,
        view=view,
        with_background=with_background,
        width=width,
        height=height,
        allow_fallback=allow_fallback,
        mime_type=mime_type,
        expires_in_seconds=expires_in_seconds,
    )
    image_url = _build_public_image_url(token)
    short_image_url, short_route_path = _build_short_public_link(image_url)
    effective_url = short_image_url or image_url
    result = {
        "sku": sku,
        "language": language,
        "workspace_id": workspace_id,
        "view": view,
        "with_background": with_background,
        "width": width,
        "height": height,
        "allow_fallback": allow_fallback,
        "mime_type": mime_type or "image/webp",
        "expires_at_unix": exp,
        "image_route_path": short_route_path or f"{IMAGE_ROUTE_PATH}/{token}",
        "image_url": effective_url,
        "markdown_image": f"![{sku}]({effective_url})" if effective_url else None,
        "markdown_link": f"[Open image]({effective_url})" if effective_url else None,
        "assistant_hint": (
            "Show markdown_image to render the image in the chat, or markdown_link if you want a clickable link only."
        ),
    }
    if not image_url:
        result["warning"] = (
            "MCP_URL (or PUBLIC_BASE_URL) is not configured. Set it to your public HTTPS origin, for example "
            "https://mcpfamilies.prototype.prolicht.digital"
        )
    return result


@mcp.tool
def get_article_downloads(
    sku: ArticleSku,
    language: Language = "en",
    workspace_id: WorkspaceId = None,
    expires_in_seconds: ExpiresInSeconds = 900,
):
    """List available downloads for one exact article SKU via /articles/{sku}/downloads and add clickable short links."""
    result = articles_backend.get_article_downloads(
        sku=sku,
        language=language,
        workspace_id=workspace_id,
    )
    if not isinstance(result, dict) or result.get("error"):
        return result

    links: list[dict] = []
    seen_download_types: set[str] = set()
    _attach_download_links_to_listing(
        result,
        sku=sku,
        language=language,
        workspace_id=workspace_id,
        expires_in_seconds=expires_in_seconds,
        links=links,
        seen_download_types=seen_download_types,
    )

    if not seen_download_types:
        discovered_types: set[str] = set()
        _collect_download_types_from_listing(result, discovered_types)
        for download_type in sorted(discovered_types):
            payload = _build_short_download_link_payload(
                sku=sku,
                download_type=download_type,
                language=language,
                workspace_id=workspace_id,
                expires_in_seconds=expires_in_seconds,
                label=download_type,
            )
            links.append(
                {
                    "download_type": download_type,
                    "label": download_type,
                    "download_url": payload.get("download_url"),
                    "download_route_path": payload.get("download_route_path"),
                    "markdown_link": payload.get("markdown_link"),
                    "expires_at_unix": payload.get("expires_at_unix"),
                }
            )

    deduped_links: list[dict] = []
    seen: set[str] = set()
    for item in links:
        download_type = str(item.get("download_type") or "").strip().lower()
        if not download_type or download_type in seen:
            continue
        seen.add(download_type)
        deduped_links.append(item)

    result["download_links"] = {
        item["download_type"]: item.get("download_url")
        for item in deduped_links
        if item.get("download_url")
    }
    result["download_markdown_links"] = {
        item["download_type"]: item.get("markdown_link")
        for item in deduped_links
        if item.get("markdown_link")
    }
    result["download_link_items"] = deduped_links
    result["assistant_hint"] = (
        "Show the markdown links or download URLs directly to the user. "
        "These are short public links that resolve to the signed MCP download URLs."
    )
    return result


@mcp.tool
def get_article_download_file(
    sku: ArticleSku,
    download_type: DownloadType,
    language: Language = "en",
    workspace_id: WorkspaceId = None,
    include_base64: Annotated[
        bool,
        Field(
            description=(
                "If true, inline the native file as base64 when it is smaller than max_inline_bytes. Default false."
            )
        ),
    ] = False,
    max_inline_bytes: Annotated[
        int,
        Field(
            ge=1,
            le=10000000,
            description=(
                "Maximum file size to inline as base64 when include_base64=true. Default: 2000000 bytes."
            ),
        ),
    ] = 2000000,
    expires_in_seconds: ExpiresInSeconds = 900,
):
    """Fetch one native article download and return a public, time-limited HTTPS link.

    Preferred for OpenWebUI chat usage: the response includes `download_url` and `markdown_link`,
    which the assistant can show directly to the user.
    """
    result = articles_backend.get_article_download_file(
        sku=sku,
        download_type=download_type,
        language=language,
        workspace_id=workspace_id,
        include_base64=include_base64,
        max_inline_bytes=max_inline_bytes,
    )
    if not isinstance(result, dict) or result.get("error"):
        return result

    token, exp = _build_article_download_token(
        sku=sku,
        download_type=download_type,
        language=language,
        workspace_id=workspace_id,
        expires_in_seconds=expires_in_seconds,
    )
    long_download_url = _build_public_download_url(token)
    download_url, short_route_path = _build_short_public_link(long_download_url)

    label = result.get("filename") or f"{sku}_{download_type}"
    effective_url = download_url or long_download_url
    result["expires_at_unix"] = exp
    result["download_route_path"] = short_route_path or f"{DOWNLOAD_ROUTE_PATH}/{token}"
    result["download_url"] = effective_url
    result["markdown_link"] = (
        f"[{label}]({effective_url})" if effective_url else None
    )
    result["assistant_hint"] = (
        "Show the user the markdown_link as the clickable download link. "
        "Do not dump content_base64 unless explicitly asked."
    )
    if not download_url:
        result["warning"] = (
            "MCP_URL (or PUBLIC_BASE_URL) is not configured. Set it to your public HTTPS origin, for example "
            "https://mcpfamilies.prototype.prolicht.digital"
        )
    return result


@mcp.tool
def create_zip_from_article_downloads(
    download_references: ZipDownloadReferences,
    zip_filename: ZipFilename = None,
    expires_in_seconds: ExpiresInSeconds = 900,
):
    """Create one short, signed, time-limited MCP ZIP download URL from multiple article download links.

    Use this only after calling get_article_download_file for each file you want to bundle.
    Pass the returned download_url or download_route_path values here. This tool validates those MCP download links,
    resolves each SKU through the lazily loaded ZIP context resolver, stores the ZIP request server-side,
    and returns one short ZIP download URL for the combined archive.

    ZIP structure:
    - Each downloaded file is placed into a product subdirectory.
    - Folder pattern: PRODUCT NAME / MOUNTING TYPE / LIGHTING CATEGORY / SYSTEM NAME (only when present).
    """
    references = [str(x or "").strip() for x in (download_references or []) if str(x or "").strip()]
    if not references:
        return {"error": "download_references must contain at least one MCP download URL or route path"}
    if len(references) > ZIP_MAX_FILES:
        return {"error": f"too_many_files_requested; maximum is {ZIP_MAX_FILES}", "max_files": ZIP_MAX_FILES}
    files, rejected = _resolve_zip_entries_from_download_references(references)
    if not files:
        return {"error": "no_valid_mcp_download_references", "rejected_references": rejected}
    token, exp = _build_zip_download_token(files=files, archive_name=zip_filename, expires_in_seconds=expires_in_seconds)
    long_zip_url = _build_public_zip_url(token)
    zip_url, short_route_path = _build_short_public_link(long_zip_url)
    archive_name = str(zip_filename or "downloads_bundle.zip").strip() or "downloads_bundle.zip"
    if not archive_name.lower().endswith(".zip"):
        archive_name = f"{archive_name}.zip"
    result = {
        "archive_name": archive_name,
        "file_count": len(files),
        "included_files": [
            {
                "sku": f.get("sku"),
                "download_type": f.get("download_type"),
                "language": f.get("language"),
                "workspace_id": f.get("workspace_id"),
                "zip_folder_path": f.get("zip_folder_path"),
                "product_name": f.get("product_name"),
                "mounting_type": f.get("mounting_type"),
                "lighting_category": f.get("lighting_category"),
                "system_name": f.get("system_name"),
                "resolver_source": f.get("resolver_source"),
            }
            for f in files
        ],
        "rejected_references": rejected,
        "expires_at_unix": exp,
        "zip_route_path": short_route_path or f"{ZIP_ROUTE_PATH}/{token}",
        "zip_url": zip_url or long_zip_url,
        "markdown_link": f"[{archive_name}]({zip_url or long_zip_url})" if (zip_url or long_zip_url) else None,
        "assistant_hint": "Show the markdown_link as the clickable ZIP download link.",
    }
    if not zip_url:
        result["warning"] = (
            "MCP_URL (or PUBLIC_BASE_URL) is not configured. Set it to your public HTTPS origin, for example "
            "https://mcpfamilies.prototype.prolicht.digital"
        )
    return result


@mcp.tool
def get_system_member_articles(
    system_sku: Annotated[
        str,
        Field(description="Exact system SKU for /articles/{system_sku}/system-members."),
    ],
    language: Language = "en",
):
    """Get detailed article payloads for all members of a system SKU."""
    return articles_backend.get_system_member_articles(system_sku=system_sku, language=language)


# -------------------------------------------------
# Transport
# -------------------------------------------------

app = create_streamable_http_app(server=mcp, streamable_http_path="/mcp")

# -------------------------------------------------
# Debug logging (optional)
# -------------------------------------------------
# These logs help diagnose failures that happen BEFORE tool code runs
# (e.g., Pydantic validation errors when OpenWebUI calls a tool with
# missing / misnamed arguments).
#
# Enable by setting:
#   MCP_LOG_HTTP_REQUESTS=1            (logs raw POST /mcp JSON body)
#   MCP_LOG_HTTP_MAX_BYTES=10000       (optional truncation; default 10000)
#
# NOTE: Raw request bodies can contain user prompts/chat content. Use temporarily.
if os.getenv("MCP_LOG_HTTP_REQUESTS", "0") == "1":
    try:
        import json
        import logging
        from starlette.requests import Request

        _mcp_http_logger = logging.getLogger("mcp.http")

        @app.middleware("http")  # type: ignore[misc]
        async def _log_mcp_http_request(request: Request, call_next):
            if request.url.path == "/mcp" and request.method.upper() == "POST":
                body = await request.body()
                max_bytes = int(os.getenv("MCP_LOG_HTTP_MAX_BYTES", "10000"))
                snippet = body[:max_bytes]
                try:
                    text = snippet.decode("utf-8", "replace")
                except Exception:
                    text = repr(snippet)

                tool_info = ""
                try:
                    payload = json.loads(body.decode("utf-8", "replace"))

                    def _extract(p):
                        if isinstance(p, dict):
                            params = p.get("params") or {}
                            name = params.get("name") or params.get("tool") or ""
                            args = params.get("arguments") or params.get("args") or {}
                            method = p.get("method") or ""
                            if name:
                                return f" method={method} tool={name} args_keys={list(args.keys())}"
                        return ""

                    if isinstance(payload, list):
                        for p in payload:
                            ti = _extract(p)
                            if ti:
                                tool_info = ti
                                break
                    else:
                        tool_info = _extract(payload)
                except Exception:
                    pass

                _mcp_http_logger.info(
                    "POST /mcp body (truncated %s bytes)%s: %s",
                    len(snippet),
                    tool_info,
                    text,
                )

                async def receive():
                    return {"type": "http.request", "body": body, "more_body": False}

                request = Request(request.scope, receive)
                return await call_next(request)

            return await call_next(request)

    except Exception:
        pass

# Important: do NOT build the ZIP resolver during app startup.
# The resolver can take a long time, and eager initialization makes the
# process look hung before uvicorn prints that the app is ready.
# ZIP resolution is lazy via mcp_backend_zipresolver.ensure_zip_context_resolver().
register_delivery_routes(app)
linkmanager_backend.register_link_routes(app, base_path=SHORT_LINK_ROUTE_PATH)


if __name__ == "__main__":
    ws_protocol = os.getenv("UVICORN_WS_PROTOCOL", "websockets-sansio")
    print(f"Starting MCP server on http://{HOST}:{PORT} (ws={ws_protocol})")
    uvicorn.run(app, host=HOST, port=PORT, ws=ws_protocol)
