from __future__ import annotations

import random
import sqlite3
import string
import threading
import time
from typing import Optional

from starlette.responses import JSONResponse, RedirectResponse
from starlette.routing import Route

BASE62 = string.digits + string.ascii_letters

_lock = threading.Lock()
_code_to_url: dict[str, str] = {}
_url_to_code: dict[str, str] = {}
_sqlite_path: str | None = None


def init_store(sqlite_path: str | None = None) -> None:
    global _sqlite_path
    _sqlite_path = str(sqlite_path).strip() if sqlite_path else None
    if not _sqlite_path:
        return

    conn = sqlite3.connect(_sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS short_links (
                code TEXT PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_short_links_url ON short_links(url)"
        )
        conn.commit()
    finally:
        conn.close()


def _generate_code(length: int = 6) -> str:
    return "".join(random.choice(BASE62) for _ in range(max(4, int(length or 6))))


def _store_in_sqlite(code: str, url: str) -> None:
    if not _sqlite_path:
        return
    conn = sqlite3.connect(_sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO short_links(code, url, created_at) VALUES (?, ?, ?)",
            (code, url, int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()


def shorten_url(url: str, code_length: int = 6) -> str:
    value = str(url or "").strip()
    if not value:
        raise ValueError("url must not be empty")

    with _lock:
        existing = _url_to_code.get(value)
        if existing:
            return existing

        if _sqlite_path:
            conn = sqlite3.connect(_sqlite_path)
            try:
                cur = conn.cursor()
                cur.execute("SELECT code FROM short_links WHERE url = ?", (value,))
                row = cur.fetchone()
            finally:
                conn.close()
            if row and row[0]:
                code = str(row[0])
                _url_to_code[value] = code
                _code_to_url[code] = value
                return code

        while True:
            code = _generate_code(code_length)
            if code in _code_to_url:
                continue
            if _sqlite_path:
                conn = sqlite3.connect(_sqlite_path)
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT 1 FROM short_links WHERE code = ?", (code,))
                    exists = cur.fetchone() is not None
                finally:
                    conn.close()
                if exists:
                    continue
            _code_to_url[code] = value
            _url_to_code[value] = code
            _store_in_sqlite(code, value)
            return code


def resolve_short_code(code: str) -> Optional[str]:
    key = str(code or "").strip()
    if not key:
        return None

    with _lock:
        value = _code_to_url.get(key)
        if value:
            return value

    if not _sqlite_path:
        return None

    conn = sqlite3.connect(_sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT url FROM short_links WHERE code = ?", (key,))
        row = cur.fetchone()
    finally:
        conn.close()

    if row and row[0]:
        value = str(row[0])
        with _lock:
            _code_to_url[key] = value
            _url_to_code[value] = key
        return value

    return None


def register_link_routes(app, base_path: str = "/l") -> None:
    route_path = str(base_path or "/l").rstrip("/") or "/l"

    async def _resolve(request):
        code = request.path_params.get("code", "")
        url = resolve_short_code(code)
        if not url:
            return JSONResponse({"error": "link_not_found"}, status_code=404)
        return RedirectResponse(url=url, status_code=307)

    # Support both FastAPI-style and Starlette-style app objects.
    if hasattr(app, "add_api_route"):
        app.add_api_route(f"{route_path}/{{code}}", _resolve, methods=["GET"], include_in_schema=False)
        return

    if hasattr(app, "add_route"):
        app.add_route(f"{route_path}/{{code}}", _resolve, methods=["GET"])
        return

    routes = getattr(app, "routes", None)
    if isinstance(routes, list):
        routes.append(Route(f"{route_path}/{{code}}", _resolve, methods=["GET"]))
        return

    raise TypeError(f"Unsupported app type for link routes: {type(app)!r}")
