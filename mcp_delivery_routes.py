from __future__ import annotations

import io
import os
import zipfile
from typing import Callable

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

import mcp_backend_articles as articles_backend
import mcp_backend_visuals as visuals_backend
from mcp_delivery_tokens import (
    CONFIG,
    image_extension_from_mime_type,
    normalize_zip_member_name,
    sanitize_folder_segment,
    verify_download_token,
    verify_image_token,
    verify_zip_reference,
)


async def _article_visual_public(request: Request) -> Response:
    token = request.path_params.get("token", "")
    payload = verify_image_token(token)
    if not payload:
        return JSONResponse({"error": "invalid_or_expired_token"}, status_code=404)

    sku = str(payload.get("sku") or "").strip()
    language = str(payload.get("language") or "en").strip() or "en"
    workspace_id = payload.get("workspace_id")
    view = payload.get("view")
    with_background = payload.get("with_background")
    width = payload.get("width")
    height = payload.get("height")
    allow_fallback = payload.get("allow_fallback")
    mime_type = payload.get("mime_type") or "image/webp"

    if not sku:
        return JSONResponse({"error": "invalid_token_payload"}, status_code=400)

    upstream = visuals_backend.fetch_article_visual_image(
        sku=sku,
        language=language,
        workspace_id=workspace_id,
        view=view,
        with_background=with_background,
        width=width,
        height=height,
        allow_fallback=allow_fallback,
        mime_type=mime_type,
    )
    if not isinstance(upstream, dict) or upstream.get("error"):
        status_code = int(upstream.get("status_code") or 502) if isinstance(upstream, dict) else 502
        return JSONResponse({"error": "upstream_visual_failed", "details": upstream}, status_code=status_code)

    content = upstream.get("content") or b""
    response_content_type = upstream.get("content_type") or mime_type or "image/webp"
    extension = image_extension_from_mime_type(response_content_type)
    headers = {
        "Content-Disposition": f'inline; filename="{sku}.{extension}"',
        "Cache-Control": "private, max-age=300",
        "X-Robots-Tag": "noindex, nofollow",
    }
    return Response(content=content, media_type=response_content_type, headers=headers)


async def _article_download_public(request: Request) -> Response:
    token = request.path_params.get("token", "")
    payload = verify_download_token(token)
    if not payload:
        return JSONResponse({"error": "invalid_or_expired_token"}, status_code=404)

    sku = str(payload.get("sku") or "").strip()
    download_type = str(payload.get("download_type") or "").strip()
    language = str(payload.get("language") or "en").strip() or "en"
    workspace_id = payload.get("workspace_id")

    if not sku or not download_type:
        return JSONResponse({"error": "invalid_token_payload"}, status_code=400)

    upstream = articles_backend.fetch_article_download_file(
        sku=sku,
        download_type=download_type,
        language=language,
        workspace_id=workspace_id,
    )
    if not isinstance(upstream, dict) or upstream.get("error"):
        status_code = int(upstream.get("status_code") or 502) if isinstance(upstream, dict) else 502
        return JSONResponse({"error": "upstream_download_failed", "details": upstream}, status_code=status_code)

    content = upstream.get("content") or b""
    try:
        filename = articles_backend._extract_filename(  # type: ignore[attr-defined]
            upstream.get("content_disposition"),
            f"{sku}_{download_type}",
        )
    except Exception:
        filename = f"{sku}_{download_type}"

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "private, max-age=300",
        "X-Robots-Tag": "noindex, nofollow",
    }
    content_type = upstream.get("content_type") or "application/octet-stream"
    return Response(content=content, media_type=content_type, headers=headers)


async def _article_download_zip_public(request: Request) -> Response:
    token = request.path_params.get("token", "")
    payload = verify_zip_reference(token)
    if not payload:
        return JSONResponse({"error": "invalid_or_expired_token"}, status_code=404)

    files = payload.get("files") or []
    archive_name = normalize_zip_member_name(payload.get("archive_name") or "downloads_bundle.zip", "downloads_bundle.zip")
    if not archive_name.lower().endswith(".zip"):
        archive_name = f"{archive_name}.zip"
    if not isinstance(files, list) or not files:
        return JSONResponse({"error": "invalid_token_payload"}, status_code=400)
    if len(files) > CONFIG.zip_max_files:
        return JSONResponse({"error": "too_many_files_requested", "max_files": CONFIG.zip_max_files}, status_code=400)

    zip_buffer = io.BytesIO()
    total_bytes = 0
    used_names: set[str] = set()
    try:
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for index, entry in enumerate(files, start=1):
                if not isinstance(entry, dict):
                    return JSONResponse({"error": "invalid_token_payload"}, status_code=400)
                sku = str(entry.get("sku") or "").strip()
                download_type = str(entry.get("download_type") or "").strip()
                language = str(entry.get("language") or "en").strip() or "en"
                workspace_id = entry.get("workspace_id")
                if not sku or not download_type:
                    return JSONResponse({"error": "invalid_token_payload"}, status_code=400)
                upstream = articles_backend.fetch_article_download_file(
                    sku=sku,
                    download_type=download_type,
                    language=language,
                    workspace_id=workspace_id,
                )
                if not isinstance(upstream, dict) or upstream.get("error"):
                    status_code = int(upstream.get("status_code") or 502) if isinstance(upstream, dict) else 502
                    return JSONResponse(
                        {
                            "error": "upstream_download_failed",
                            "details": upstream,
                            "failed_entry": {"sku": sku, "download_type": download_type, "index": index},
                        },
                        status_code=status_code,
                    )
                content = upstream.get("content") or b""
                total_bytes += len(content)
                if total_bytes > CONFIG.zip_max_total_bytes:
                    return JSONResponse({"error": "zip_size_limit_exceeded", "max_total_bytes": CONFIG.zip_max_total_bytes}, status_code=413)
                try:
                    filename = articles_backend._extract_filename(upstream.get("content_disposition"), f"{sku}_{download_type}")  # type: ignore[attr-defined]
                except Exception:
                    filename = f"{sku}_{download_type}"
                safe_name = normalize_zip_member_name(filename, f"{sku}_{download_type}")
                folder_path = str(entry.get("zip_folder_path") or "").strip()
                normalized_folder = "/".join(
                    [sanitize_folder_segment(part, "UNKNOWN") for part in folder_path.split("/") if str(part).strip()]
                )
                archive_member = f"{normalized_folder}/{safe_name}" if normalized_folder else safe_name
                final_name = archive_member
                base, ext = os.path.splitext(archive_member)
                counter = 2
                while final_name in used_names:
                    final_name = f"{base}_{counter}{ext}"
                    counter += 1
                used_names.add(final_name)
                zf.writestr(final_name, content)
    except Exception as exc:
        return JSONResponse({"error": "zip_creation_failed", "details": str(exc)}, status_code=500)

    headers = {
        "Content-Disposition": f'attachment; filename="{archive_name}"',
        "Cache-Control": "private, max-age=300",
        "X-Robots-Tag": "noindex, nofollow",
    }
    return Response(content=zip_buffer.getvalue(), media_type="application/zip", headers=headers)


def register_delivery_routes(app, *, initialize_zip_context_resolver: Callable[[], None] | None = None) -> None:
    app.router.routes.append(
        Route(f"{CONFIG.image_route_path}/{{token:path}}", endpoint=_article_visual_public, methods=["GET"])
    )
    app.router.routes.append(
        Route(f"{CONFIG.download_route_path}/{{token:path}}", endpoint=_article_download_public, methods=["GET"])
    )
    app.router.routes.append(
        Route(f"{CONFIG.zip_route_path}/{{token:path}}", endpoint=_article_download_zip_public, methods=["GET"])
    )
