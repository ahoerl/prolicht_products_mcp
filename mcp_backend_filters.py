from __future__ import annotations

import json
import os
import time
from typing import Dict, Any, Optional, Tuple

from mcp_backend import (
    DEFAULT_FILTERS_API_URL,
    build_filter_params,
    api_get,
)

# Filters change rarely; caching avoids repeated heavy calls and mitigates transient API slowness.
_FILTERS_CACHE_TTL_SEC = int(os.getenv("PRODUCTS_FILTERS_CACHE_TTL_SEC", "600"))  # 10 min
_filters_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _cache_key(active_filters: Optional[Dict[str, list]], lang: str) -> str:
    # Deterministic serialization (best-effort)
    af = active_filters or {}
    try:
        af_norm = {k: list(v) for k, v in sorted(af.items(), key=lambda kv: kv[0])}
        af_json = json.dumps(af_norm, ensure_ascii=False, sort_keys=True)
    except Exception:
        af_json = str(af)
    return f"lang={lang}|active_filters={af_json}"


def fetch_product_filters(
    active_filters: Optional[Dict[str, list]] = None,
    lang: str = "en",
    base_url: str = DEFAULT_FILTERS_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    """Fetch /products/filters with small TTL cache + graceful fallback."""

    key = _cache_key(active_filters, lang)

    # Serve from cache if fresh
    if _FILTERS_CACHE_TTL_SEC > 0 and key in _filters_cache:
        ts, cached = _filters_cache[key]
        if (time.time() - ts) <= _FILTERS_CACHE_TTL_SEC:
            return {"cached": True, **cached}

    params = {"lang": lang}
    if active_filters:
        params.update(build_filter_params(active_filters))

    resp = api_get(base_url, params=params, timeout=timeout)

    # If we have a cached value and the network call failed, fall back to cache.
    if isinstance(resp, dict) and resp.get("error"):
        if key in _filters_cache:
            _ts, cached = _filters_cache[key]
            return {
                "warning": "filters endpoint failed; served cached response",
                "cached": True,
                "upstream_error": resp,
                **cached,
            }
        return resp

    # Cache successful response
    if _FILTERS_CACHE_TTL_SEC > 0 and isinstance(resp, dict) and not resp.get("error"):
        _filters_cache[key] = (time.time(), resp)

    return resp


def extract_product_families(filter_response: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(filter_response, dict):
        return {"families": []}
    if filter_response.get("error"):
        return {"families": [], "error": filter_response.get("error"), "debug": filter_response}

    data = filter_response.get("data", []) or []
    for block in data:
        if block.get("id") == "product_family":
            values = block.get("attributes", {}).get("values", [])
            return {"families": values}
    return {"families": []}
