from __future__ import annotations

import base64
import os
from typing import Any, Dict, Optional

from mcp_backend import DEFAULT_VISUALS_API_URL, api_get_binary

DEFAULT_VISUAL_INLINE_LIMIT = int(os.getenv("ARTICLE_VISUAL_INLINE_LIMIT", "2000000"))


def fetch_article_visual_image(
    sku: str,
    language: str = "en",
    workspace_id: Optional[str] = None,
    view: Optional[str] = None,
    with_background: Optional[bool] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    allow_fallback: Optional[bool] = None,
    mime_type: Optional[str] = None,
    base_url: str = DEFAULT_VISUALS_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    url = f"{base_url}/image/{sku}"
    params: Dict[str, Any] = {"lang": language}
    if workspace_id:
        params["ws"] = workspace_id
    if view:
        params["view"] = view
    if with_background is not None:
        params["with_background"] = str(with_background).lower()
    if width is not None:
        params["width"] = int(width)
    if height is not None:
        params["height"] = int(height)
    if allow_fallback is not None:
        params["allow_fallback"] = str(allow_fallback).lower()
    if mime_type:
        params["mime_type"] = mime_type
    return api_get_binary(url, params=params, timeout=timeout, accept=mime_type or "image/webp,image/*,*/*")


def get_article_visual_image(
    sku: str,
    language: str = "en",
    workspace_id: Optional[str] = None,
    view: Optional[str] = None,
    with_background: Optional[bool] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    allow_fallback: Optional[bool] = None,
    mime_type: Optional[str] = None,
    include_base64: bool = False,
    max_inline_bytes: int = DEFAULT_VISUAL_INLINE_LIMIT,
    timeout=None,
) -> Dict[str, Any]:
    resp = fetch_article_visual_image(
        sku=sku,
        language=language,
        workspace_id=workspace_id,
        view=view,
        with_background=with_background,
        width=width,
        height=height,
        allow_fallback=allow_fallback,
        mime_type=mime_type,
        timeout=timeout,
    )
    if not isinstance(resp, dict) or resp.get("error"):
        return resp

    content = resp.get("content") or b""
    content_type = resp.get("content_type") or mime_type or "image/webp"

    out: Dict[str, Any] = {
        "sku": sku,
        "language": language,
        "workspace_id": workspace_id,
        "view": view,
        "with_background": with_background,
        "width": width,
        "height": height,
        "allow_fallback": allow_fallback,
        "mime_type": content_type,
        "size_bytes": len(content),
    }

    if not include_base64:
        out["image_hint"] = (
            "Set include_base64=true if you need inline image content and the payload is small enough."
        )
        return out

    if len(content) > max_inline_bytes:
        out["error"] = "Image too large to inline as base64"
        out["max_inline_bytes"] = max_inline_bytes
        return out

    out["content_base64"] = base64.b64encode(content).decode("ascii")
    return out
