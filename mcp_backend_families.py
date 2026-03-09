from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

# IMPORTANT:
# This backend module must NOT import the MCP tool-layer (mcp_server.py).
# It should only talk to the API-facing backend helpers.
#
# We use:
# - mcp_backend_filters.fetch_product_filters + extract_product_families  (to get family keys)
# - mcp_backend_products.search_products                                (to get products per family)
#
# This avoids circular imports and makes the module usable both from MCP tools
# and from tests/scripts.

import mcp_backend_filters as filters_backend
import mcp_backend_products as products_backend


# -----------------------------------------------------------------------------
# Families -> Variants aggregation (API-only)
# -----------------------------------------------------------------------------
#
# Why this file exists:
# - /products/filters exposes ONLY the family keys, not which products belong to a family.
# - If an LLM needs 'family -> products' mapping, we must query /products and group results.
#
# Output is pruned/compact so tool output does not blow up the context window.
#


def _key_or_value(x: Union[str, Dict[str, Any], None]) -> Optional[str]:
    """Normalize API fields that sometimes come as strings or {key,label,value}."""
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


def prune_products_list(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """Return the same compact product-card schema used by search_products.

    This keeps grouped-family answers consistent with the main product search tool so an LLM can
    reuse the same field expectations across both tools.
    """
    return products_backend.prune_product_list(api_response)


def _fetch_all_family_keys(language: str = "en") -> List[str]:
    """Return all product_family keys from /products/filters."""
    raw = filters_backend.fetch_product_filters(active_filters=None, lang=language)
    families_block = filters_backend.extract_product_families(raw)
    values = families_block.get("families", []) or []
    # values are objects like {key,value}; we return just the keys.
    keys: List[str] = []
    for v in values:
        if isinstance(v, dict):
            k = v.get("key")
            if isinstance(k, str) and k.strip():
                keys.append(k.strip())
        elif isinstance(v, str) and v.strip():
            keys.append(v.strip())
    return keys


def get_products_grouped_by_family(
    family_ids: Optional[List[str]] = None,
    language: str = "en",
    per_page: int = 50,
    max_variants_per_family: int = 500,
    max_total_variants: int = 3000,
    include_raw: bool = False,
) -> Dict[str, Any]:
    """Aggregate product variants grouped by product family using ONLY the WebAPI."""
    fams = family_ids or _fetch_all_family_keys(language=language)

    families_out: Dict[str, Dict[str, Any]] = {}
    total_variants = 0
    raw_pages: Dict[str, List[Dict[str, Any]]] = {}

    for fam in fams:
        if total_variants >= max_total_variants:
            break

        variants: List[Dict[str, Any]] = []
        offset = 0

        while True:
            if len(variants) >= max_variants_per_family or total_variants >= max_total_variants:
                break

            raw = products_backend.search_products(
                filters={"product_family": [fam]},
                lang=language,
                limit=per_page,
                offset=offset,
            )

            if include_raw:
                raw_pages.setdefault(fam, []).append(raw)

            pruned = prune_products_list(raw)
            page_results = pruned.get("results", []) or []
            if not page_results:
                break

            for r in page_results:
                if len(variants) >= max_variants_per_family or total_variants >= max_total_variants:
                    break
                variants.append(r)
                total_variants += 1

            # Stop paging if we likely reached the last page.
            meta = raw.get("meta", {}) or {}
            limit = meta.get("limit", per_page)
            returned = meta.get("items", None) or len(raw.get("data", []) or [])
            if returned < limit:
                break

            offset += per_page

        families_out[fam] = {
            "family_id": fam,
            "variant_count": len(variants),
            "variants": variants,
        }

    response: Dict[str, Any] = {
        "meta": {
            "lang": language,
            "families_requested": len(fams),
            "families_returned": len(families_out),
            "total_variants_returned": total_variants,
            "per_page": per_page,
            "max_variants_per_family": max_variants_per_family,
            "max_total_variants": max_total_variants,
        },
        "families": families_out,
    }

    if include_raw:
        response["raw_pages"] = raw_pages

    return response
