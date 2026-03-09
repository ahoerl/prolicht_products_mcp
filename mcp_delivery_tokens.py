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
from dataclasses import dataclass

import mcp_backend_linkmanager as linkmanager_backend
import mcp_backend_zipresolver as zipresolver_backend


@dataclass(frozen=True)
class DeliveryConfig:
    public_url: str
    download_url_secret: str
    download_url_ttl_seconds: int
    download_route_path: str
    image_url_secret: str
    image_url_ttl_seconds: int
    image_route_path: str
    zip_url_secret: str
    zip_url_ttl_seconds: int
    zip_route_path: str
    zip_max_files: int
    zip_max_total_bytes: int
    zip_token_storage_dir: str

    @classmethod
    def from_env(cls) -> "DeliveryConfig":
        public_url = os.getenv("MCP_URL", os.getenv("PUBLIC_BASE_URL", "")).rstrip("/")
        download_url_secret = os.getenv("MCP_DOWNLOAD_URL_SECRET", "change-me")
        download_url_ttl_seconds = int(os.getenv("MCP_DOWNLOAD_URL_TTL_SECONDS", "900"))
        download_route_path = os.getenv("MCP_DOWNLOAD_ROUTE_PATH", "/mcp/downloads/articles").rstrip("/")
        image_url_secret = os.getenv("MCP_IMAGE_URL_SECRET", download_url_secret)
        image_url_ttl_seconds = int(os.getenv("MCP_IMAGE_URL_TTL_SECONDS", str(download_url_ttl_seconds)))
        image_route_path = os.getenv("MCP_IMAGE_ROUTE_PATH", "/mcpvisuals/image").rstrip("/")
        zip_url_secret = os.getenv("MCP_ZIP_URL_SECRET", download_url_secret)
        zip_url_ttl_seconds = int(os.getenv("MCP_ZIP_URL_TTL_SECONDS", str(download_url_ttl_seconds)))
        zip_route_path = os.getenv("MCP_ZIP_ROUTE_PATH", "/mcp/downloads/zips").rstrip("/")
        zip_max_files = int(os.getenv("MCP_ZIP_MAX_FILES", "25"))
        zip_max_total_bytes = int(os.getenv("MCP_ZIP_MAX_TOTAL_BYTES", "104857600"))
        zip_token_storage_dir = os.getenv("MCP_ZIP_TOKEN_STORAGE_DIR", "/tmp/prolicht_mcp_zip_tokens")
        return cls(
            public_url=public_url,
            download_url_secret=download_url_secret,
            download_url_ttl_seconds=download_url_ttl_seconds,
            download_route_path=download_route_path,
            image_url_secret=image_url_secret,
            image_url_ttl_seconds=image_url_ttl_seconds,
            image_route_path=image_route_path,
            zip_url_secret=zip_url_secret,
            zip_url_ttl_seconds=zip_url_ttl_seconds,
            zip_route_path=zip_route_path,
            zip_max_files=zip_max_files,
            zip_max_total_bytes=zip_max_total_bytes,
            zip_token_storage_dir=zip_token_storage_dir,
        )


CONFIG = DeliveryConfig.from_env()


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


def build_public_download_url(token: str) -> str | None:
    if not CONFIG.public_url:
        return None
    return f"{CONFIG.public_url}{CONFIG.download_route_path}/{token}"


def build_public_image_url(token: str) -> str | None:
    if not CONFIG.public_url:
        return None
    return f"{CONFIG.public_url}{CONFIG.image_route_path}/{token}"


def build_public_zip_url(token: str) -> str | None:
    if not CONFIG.public_url:
        return None
    return f"{CONFIG.public_url}{CONFIG.zip_route_path}/{token}"


def verify_download_token(token: str) -> dict | None:
    return _verify_signed_token(token, CONFIG.download_url_secret)


def verify_image_token(token: str) -> dict | None:
    return _verify_signed_token(token, CONFIG.image_url_secret)


def verify_zip_token(token: str) -> dict | None:
    return _verify_signed_token(token, CONFIG.zip_url_secret)


def build_article_download_token(
    sku: str,
    download_type: str,
    language: str = "en",
    workspace_id: str | None = None,
    expires_in_seconds: int | None = None,
) -> tuple[str, int]:
    ttl = int(expires_in_seconds or CONFIG.download_url_ttl_seconds)
    ttl = max(60, min(ttl, 86400))
    exp = int(time.time()) + ttl
    payload = {
        "sku": sku,
        "download_type": download_type,
        "language": language,
        "workspace_id": workspace_id,
        "exp": exp,
    }
    return _sign_payload(payload, CONFIG.download_url_secret), exp


def build_article_image_token(
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
    ttl = int(expires_in_seconds or CONFIG.image_url_ttl_seconds)
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
    return _sign_payload(payload, CONFIG.image_url_secret), exp


def build_article_image_link_payload(
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
    token, exp = build_article_image_token(
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
    image_url = build_public_image_url(token)
    route_path = f"{CONFIG.image_route_path}/{token}"
    display_url = image_url
    display_route_path = route_path
    short_link_code = None
    if image_url:
        try:
            short_url, short_route_path, short_code = linkmanager_backend.shorten_public_url(image_url, expires_at=exp)
            if short_url:
                display_url = short_url
            if short_route_path:
                display_route_path = short_route_path
            short_link_code = short_code
        except Exception:
            pass
    payload = {
        "primary_article_sku": sku_value,
        "image_route_path": display_route_path,
        "image_url": display_url,
        "markdown_image": f"![{sku_value}]({display_url})" if display_url else None,
        "markdown_link": f"[Open image]({display_url})" if display_url else None,
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
    if short_link_code:
        payload["short_link_code"] = short_link_code
    return payload


def attach_primary_image_links_to_product_list(
    results: list[dict],
    language: str,
    workspace_id: str | None = None,
) -> None:
    for result in results:
        if not isinstance(result, dict):
            continue
        primary = build_article_image_link_payload(
            sku=result.get("primary_article_sku"),
            language=language,
            workspace_id=workspace_id,
            mime_type="image/webp",
        )
        if primary:
            result["primary_image"] = primary


def image_extension_from_mime_type(value: str | None) -> str:
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


def _ensure_zip_token_storage_dir() -> None:
    os.makedirs(CONFIG.zip_token_storage_dir, exist_ok=True)


def _zip_token_storage_path(short_token: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "", str(short_token or ""))
    return os.path.join(CONFIG.zip_token_storage_dir, f"{safe}.json")


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


def verify_zip_reference(token: str) -> dict | None:
    payload = _load_stored_zip_payload(token)
    if payload:
        return payload
    return verify_zip_token(token)


def normalize_zip_member_name(name: str, fallback: str) -> str:
    candidate = str(name or "").strip() or fallback
    candidate = candidate.replace("\\", "/").split("/")[-1]
    safe = "".join(ch if ch not in '<>:"|?*' else '_' for ch in candidate).strip().strip('.')
    return safe or fallback


def sanitize_folder_segment(value: str | None, fallback: str) -> str:
    raw = str(value or "").strip() or fallback
    raw = raw.replace("\\", "/").replace("/", " - ")
    safe = "".join(ch if ch not in '<>:"|?*' else "_" for ch in raw)
    safe = " ".join(safe.split()).strip().strip(".")
    return safe or fallback


def extract_token_from_download_reference(reference: str) -> str | None:
    value = str(reference or "").strip()
    if not value:
        return None
    expanded = linkmanager_backend.expand_short_reference(value)
    if expanded:
        value = expanded
    if value.startswith("http://") or value.startswith("https://"):
        try:
            parsed = urllib.parse.urlparse(value)
            path = parsed.path or ""
        except Exception:
            return None
    else:
        path = value
    normalized_path = posixpath.normpath(path)
    prefix = f"{CONFIG.download_route_path}/"
    if not normalized_path.startswith(prefix):
        return None
    token = normalized_path[len(prefix):].strip("/")
    return token or None


def build_zip_download_token(
    files: list[dict],
    archive_name: str | None = None,
    expires_in_seconds: int | None = None,
) -> tuple[str, int]:
    ttl = int(expires_in_seconds or CONFIG.zip_url_ttl_seconds)
    ttl = max(60, min(ttl, 86400))
    exp = int(time.time()) + ttl
    archive = str(archive_name or "downloads_bundle.zip").strip() or "downloads_bundle.zip"
    if not archive.lower().endswith(".zip"):
        archive = f"{archive}.zip"
    payload = {"files": files, "archive_name": archive, "exp": exp}
    short_token = _store_zip_payload(payload)
    return short_token, exp


def resolve_zip_entries_from_download_references(references: list[str]) -> tuple[list[dict], list[dict]]:
    valid_entries: list[dict] = []
    rejected: list[dict] = []
    for reference in references:
        token = extract_token_from_download_reference(reference)
        if not token:
            rejected.append({"reference": reference, "reason": "not_an_mcp_article_download_url"})
            continue
        payload = verify_download_token(token)
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
