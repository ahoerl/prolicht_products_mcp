from __future__ import annotations

import base64
import os
import re
from typing import Any, Dict, Optional

from mcp_backend import DEFAULT_ARTICLES_API_URL, api_get, api_get_binary

DEFAULT_ARTICLE_DOWNLOAD_INLINE_LIMIT = int(
    os.getenv("ARTICLE_DOWNLOAD_INLINE_LIMIT", "2000000")
)


def _prune_article_payload(node: Any) -> Any:
    if isinstance(node, list):
        return [_prune_article_payload(item) for item in node]

    if not isinstance(node, dict):
        return node

    attrs = node.get("attributes")
    if not isinstance(attrs, dict):
        return node

    preferred_keys = [
        "sku",
        "system_sku",
        "id",
        "name",
        "title",
        "subtitle",
        "description",
        "short_description",
        "technical_representation",
        "image_gallery",
        "images",
        "specifications",
        "features",
        "downloads",
        "accessories",
        "contained_article_skus",
    ]
    pruned_attrs = {k: attrs.get(k) for k in preferred_keys if k in attrs}
    if not pruned_attrs:
        pruned_attrs = attrs

    out: Dict[str, Any] = {
        "id": node.get("id"),
        "type": node.get("type"),
        "attributes": pruned_attrs,
    }
    relationships = node.get("relationships")
    if isinstance(relationships, dict) and relationships:
        out["relationships"] = relationships
    return out


def _prune_response(resp: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(resp, dict) or resp.get("error"):
        return resp

    data = resp.get("data")
    included = resp.get("included")

    out: Dict[str, Any] = {"data": _prune_article_payload(data)}
    if "meta" in resp:
        out["meta"] = resp.get("meta")
    if isinstance(included, list) and included:
        out["included"] = _prune_article_payload(included)
    return out


def _extract_filename(content_disposition: Optional[str], fallback: str) -> str:
    if not content_disposition:
        return fallback

    star_match = re.search(r"filename\*=UTF-8''([^;]+)", content_disposition, flags=re.IGNORECASE)
    if star_match:
        return star_match.group(1).strip().strip('"')

    plain_match = re.search(r'filename="?([^";]+)"?', content_disposition, flags=re.IGNORECASE)
    if plain_match:
        return plain_match.group(1).strip()

    return fallback


def fetch_article_details(
    sku: str,
    language: str = "en",
    base_url: str = DEFAULT_ARTICLES_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    url = f"{base_url}/{sku}"
    return api_get(url, params={"lang": language}, timeout=timeout)


def get_article_details(
    sku: str,
    language: str = "en",
    timeout=None,
) -> Dict[str, Any]:
    return _prune_response(fetch_article_details(sku=sku, language=language, timeout=timeout))


def fetch_article_accessories(
    sku: str,
    language: str = "en",
    include_optional: Optional[bool] = None,
    base_url: str = DEFAULT_ARTICLES_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    url = f"{base_url}/{sku}/accessories"
    params: Dict[str, Any] = {"lang": language}
    if include_optional is not None:
        params["include_optional"] = str(include_optional).lower()
    return api_get(url, params=params, timeout=timeout)


def get_article_accessories(
    sku: str,
    language: str = "en",
    include_optional: Optional[bool] = None,
    timeout=None,
) -> Dict[str, Any]:
    return _prune_response(
        fetch_article_accessories(
            sku=sku,
            language=language,
            include_optional=include_optional,
            timeout=timeout,
        )
    )


def fetch_article_downloads(
    sku: str,
    language: str = "en",
    workspace_id: Optional[str] = None,
    base_url: str = DEFAULT_ARTICLES_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    url = f"{base_url}/{sku}/downloads"
    params: Dict[str, Any] = {"lang": language}
    if workspace_id:
        params["workspace_id"] = workspace_id
    return api_get(url, params=params, timeout=timeout)


def get_article_downloads(
    sku: str,
    language: str = "en",
    workspace_id: Optional[str] = None,
    timeout=None,
) -> Dict[str, Any]:
    return _prune_response(
        fetch_article_downloads(
            sku=sku,
            language=language,
            workspace_id=workspace_id,
            timeout=timeout,
        )
    )


def fetch_article_download_file(
    sku: str,
    download_type: str,
    language: str = "en",
    workspace_id: Optional[str] = None,
    base_url: str = DEFAULT_ARTICLES_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    url = f"{base_url}/{sku}/downloads/{download_type}"
    params: Dict[str, Any] = {"lang": language}
    if workspace_id:
        params["workspace_id"] = workspace_id
    return api_get_binary(url, params=params, timeout=timeout)


def get_article_download_file(
    sku: str,
    download_type: str,
    language: str = "en",
    workspace_id: Optional[str] = None,
    include_base64: bool = False,
    max_inline_bytes: int = DEFAULT_ARTICLE_DOWNLOAD_INLINE_LIMIT,
    timeout=None,
) -> Dict[str, Any]:
    resp = fetch_article_download_file(
        sku=sku,
        download_type=download_type,
        language=language,
        workspace_id=workspace_id,
        timeout=timeout,
    )
    if not isinstance(resp, dict) or resp.get("error"):
        return resp

    content = resp.get("content") or b""
    size_bytes = len(content)
    filename = _extract_filename(
        resp.get("content_disposition"),
        f"{sku}_{download_type}",
    )

    out: Dict[str, Any] = {
        "sku": sku,
        "download_type": download_type,
        "filename": filename,
        "content_type": resp.get("content_type"),
        "size_bytes": size_bytes,
        "workspace_id": workspace_id,
        "language": language,
    }

    if not include_base64:
        out["download_hint"] = (
            "Set include_base64=true if you need inline file content and the payload is small enough."
        )
        return out

    if size_bytes > max_inline_bytes:
        out["error"] = "Download too large to inline as base64"
        out["max_inline_bytes"] = max_inline_bytes
        return out

    out["content_base64"] = base64.b64encode(content).decode("ascii")
    return out


def fetch_system_member_articles(
    system_sku: str,
    language: str = "en",
    base_url: str = DEFAULT_ARTICLES_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    url = f"{base_url}/{system_sku}/system-members"
    return api_get(url, params={"lang": language}, timeout=timeout)


def get_system_member_articles(
    system_sku: str,
    language: str = "en",
    timeout=None,
) -> Dict[str, Any]:
    return _prune_response(
        fetch_system_member_articles(
            system_sku=system_sku,
            language=language,
            timeout=timeout,
        )
    )
