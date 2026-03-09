from __future__ import annotations
from typing import Dict, Any, Optional, List, Union

from mcp_backend import (
    DEFAULT_PRODUCTS_API_URL,
    build_filter_params,
    derive_system_sku,
    api_get,
)


# -------------------------------------------------------
# Search (LIST endpoint) + pruning for LLM-friendly output
# -------------------------------------------------------
#
# Why prune?
# - The /products endpoint can return extremely large payloads (100k+ characters).
# - LLMs in OpenWebUI become unreliable when tool outputs are huge.
# - For search/browsing, we only need a compact "card" per product variant.
#
# IMPORTANT:
# - For technical details, call the ITEM endpoint via get_product_details (tool),
#   which already returns a pruned technical structure.
#


def _key_or_value(x: Union[str, Dict[str, Any], None]) -> Optional[str]:
    """Normalize API fields that sometimes come as strings or as {key,label,value} objects."""
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


def _key_list(items: Any) -> List[str]:
    """Return list of machine keys from arrays like [{'key': 'IP20', ...}, ...]."""
    if not items:
        return []
    out: List[str] = []
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict):
                k = it.get("key")
                if isinstance(k, str) and k.strip():
                    out.append(k.strip())
            elif isinstance(it, str) and it.strip():
                out.append(it.strip())
    return out


def _range_min_max(rng: Any) -> Dict[str, Optional[float]]:
    """Extract min/max from ranges like [{'key': 10.0, ...}, {'key': 50.0, ...}]."""
    if not isinstance(rng, list) or not rng:
        return {"min": None, "max": None}
    vals: List[float] = []
    for it in rng:
        if isinstance(it, dict) and isinstance(it.get("key"), (int, float)):
            vals.append(float(it["key"]))
    if not vals:
        return {"min": None, "max": None}
    return {"min": min(vals), "max": max(vals)}


def _first_nonempty_sku(items: Any) -> Optional[str]:
    if not isinstance(items, list):
        return None
    for raw in items:
        s = str(raw).strip()
        if s:
            return s
    return None


def prune_product_list(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """Prune /products (LIST) response into a compact schema for LLMs.

    Notes:
    - `is_system` is always present (default: False).
    - `applications` / `segments` / `target_groups` are included (when present) so that
      marketing filters remain explainable in pruned results.
    - For *systems* (is_system==True) we expose a derived `system_sku` (needed to fetch inserts)
      and omit technical ranges that do not apply to the system container itself.
    - For *system inserts/components* returned by `filter[system_sku]=...`, the upstream payload
      contains `attributes.system_sku` (the system SKU they belong to). We keep this field so
      the relationship remains visible after pruning.
    """
    meta = api_response.get("meta", {}) or {}
    data = api_response.get("data", []) or []

    results: List[Dict[str, Any]] = []

    for item in data:
        attrs = item.get("attributes", {}) or {}

        mounting_key = _key_or_value(attrs.get("mounting_type"))
        lighting_key = _key_or_value(attrs.get("lighting_category"))

        electrical = attrs.get("electrical", {}) or {}
        lighting = attrs.get("lighting", {}) or {}
        categ = attrs.get("categorization", {}) or {}

        is_system = bool(attrs.get("is_system", False))
        contained_skus = attrs.get("contained_article_skus") or []
        primary_article_sku = _first_nonempty_sku(contained_skus)

        # Upstream may return system_sku for inserts (non-systems)
        upstream_system_sku = attrs.get("system_sku")
        if isinstance(upstream_system_sku, str):
            upstream_system_sku = upstream_system_sku.strip() or None
        else:
            upstream_system_sku = None

        entry: Dict[str, Any] = {
            "product_name": attrs.get("id"),
            "numeric_product_id": attrs.get("numeric_product_id"),
            "product_family_id": attrs.get("product_family_id"),
            "mounting_type": mounting_key,
            "lighting_category": lighting_key,
            "is_system": is_system,
            # still useful on both systems & inserts
            "product_categories": _key_list(categ.get("product_categories")),
            # marketing filters (make filter matches visible in results)
            "applications": _key_list(categ.get("applications")),
            "segments": _key_list(categ.get("segments")),
            "target_groups": _key_list(categ.get("target_groups")),
            "primary_article_sku": primary_article_sku,
        }

        if is_system:
            # Derive from contained_article_skus (8150-... prefix only)
            entry["system_sku"] = derive_system_sku(contained_skus)
            # Do NOT include the following fields for system containers:
            # - ip_rates, dimmability, luminous_flux_lm, power_w, lumen_per_watt
        else:
            # For inserts / luminaires: keep technical summary
            entry["ip_rates"] = _key_list(categ.get("ip_rates"))
            entry["dimmability"] = _key_list(electrical.get("dimmability_types"))
            entry["luminous_flux_lm"] = _range_min_max(lighting.get("real_luminous_flux_range"))
            entry["power_w"] = _range_min_max(electrical.get("total_power_range"))
            entry["lumen_per_watt"] = _range_min_max(lighting.get("lumen_per_watt_range"))

            # Keep system_sku if upstream provides it (inserts belong to a system)
            if upstream_system_sku:
                entry["system_sku"] = upstream_system_sku

            # contained_article_skus can be large; keep only count
            entry["contained_article_skus_count"] = len(contained_skus)

        results.append(entry)

    return {
        "meta": {
            "lang": meta.get("lang"),
            "limit": meta.get("limit"),
            "offset": meta.get("offset"),
            "total_items": meta.get("total_items"),
            "returned_items": len(results),
        },
        "results": results,
    }

def search_products(
    filters: Dict[str, list],
    lang: str = "en",
    limit: int = 50,
    offset: int = 0,
    sort: Optional[str] = None,
    base_url: str = DEFAULT_PRODUCTS_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    """Call /products (LIST endpoint) and return RAW API response."""

    params = {
        "lang": lang,
        "page[limit]": limit,
        "page[offset]": offset,
    }

    if sort:
        params["sort"] = sort

    params.update(build_filter_params(filters))

    return api_get(base_url, params=params, timeout=timeout)
