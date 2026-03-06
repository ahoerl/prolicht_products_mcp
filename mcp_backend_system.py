from __future__ import annotations

from typing import Iterable, Optional, List

from mcp_backend import derive_system_sku


def _key_or_value(x):
    """Small normalizer for {key,label,value} objects or strings."""
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip()
        return s or None
    if isinstance(x, dict):
        k = x.get("key")
        if isinstance(k, str) and k.strip():
            return k.strip()
        v = x.get("value")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def resolve_system_sku(
    *,
    product_family_id: str,
    product_name: str,
    mounting_type: str,
    language: str = "en",
    limit: int = 200,
    base_url: str | None = None,
    timeout=None,
) -> Optional[str]:
    """Resolve a system's `system_sku` from human identifiers.

    We query /products using family + mounting_type and then select the system entry matching
    the product display name (attributes.id) and is_system==true.

    Returns:
      - system_sku string (e.g. '8150-088') or None
    """
    from mcp_backend import DEFAULT_PRODUCTS_API_URL, build_filter_params, api_get

    if base_url is None:
        base_url = DEFAULT_PRODUCTS_API_URL

    # First try: strict filters (fast & deterministic if filter[product] matches attributes.id keys)
    strict_filters = {
        "product_family": [product_family_id],
        "mounting_type": [mounting_type],
        "product": [product_name],
    }

    params = {"lang": language, "page[limit]": limit}
    params.update(build_filter_params(strict_filters))
    try:
        raw = api_get(base_url, params=params, timeout=timeout)
    except Exception:
        raw = {"data": []}

    candidates = raw.get("data") or []

    # Fallback: if nothing found, relax product filter and match by attributes.id manually.
    if not candidates:
        relaxed_filters = {
            "product_family": [product_family_id],
            "mounting_type": [mounting_type],
        }
        params = {"lang": language, "page[limit]": limit}
        params.update(build_filter_params(relaxed_filters))
        raw = api_get(base_url, params=params, timeout=timeout)
        candidates = raw.get("data") or []

    wanted = (product_name or "").strip().lower()

    for item in candidates:
        attrs = item.get("attributes", {}) or {}
        if not bool(attrs.get("is_system", False)):
            continue
        mt = _key_or_value(attrs.get("mounting_type"))
        if mt and mounting_type and mt != mounting_type:
            continue
        pid = str(attrs.get("id") or "").strip().lower()
        if wanted and pid != wanted:
            continue
        return derive_system_sku(attrs.get("contained_article_skus") or [])

    return None


def get_system_inserts(
    *,
    product_family_id: str,
    product_name: str,
    mounting_type: str,
    language: str = "en",
    limit: int = 200,
    timeout=None,
) -> dict:
    """Fetch system inserts/components for a given system variant (without requiring the user to provide SKUs).

    Flow:
      1) resolve system_sku for the *system container* via /products
      2) call /products again with filter[system_sku]=<resolved> to get inserts/components
      3) return a pruned list (same pruning as search_products)
    """
    from mcp_backend import DEFAULT_PRODUCTS_API_URL, build_filter_params, api_get

    system_sku = resolve_system_sku(
        product_family_id=product_family_id,
        product_name=product_name,
        mounting_type=mounting_type,
        language=language,
        limit=limit,
        timeout=timeout,
    )

    if not system_sku:
        return {
            "error": "Could not resolve system_sku for the requested system variant.",
            "debug": {
                "product_family_id": product_family_id,
                "product_name": product_name,
                "mounting_type": mounting_type,
            },
        }

    params = {"lang": language, "page[limit]": limit}
    params.update(build_filter_params({"system_sku": [system_sku]}))
    raw = api_get(DEFAULT_PRODUCTS_API_URL, params=params, timeout=timeout)

    # Local import to avoid circular import at module import time
    import mcp_backend_products as products_backend

    pruned = products_backend.prune_product_list(raw)

    return {
        "system": {
            "product_family_id": product_family_id,
            "product_name": product_name,
            "mounting_type": mounting_type,
            "system_sku": system_sku,
        },
        "inserts": pruned,
    }