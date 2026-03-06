from __future__ import annotations
import os
from typing import Dict, Any, Optional, List, Union

from mcp_backend import (
    DEFAULT_PRODUCTS_API_URL,
    build_filter_params,
    derive_system_sku,
    api_get,
)


def _key_or_value(x: Union[str, Dict[str, Any], None]) -> Optional[str]:
    """Normalize API fields that sometimes come as strings or as {key,label,value} objects.

    The /products list endpoint may return categorization fields as objects:
      {'key': 'TRACK', 'label': '...', 'value': '...'}.
    The ITEM endpoint expects the path parameters to be the machine key (e.g. 'TRACK').
    """
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip()
        return s or None
    if isinstance(x, dict):
        # Prefer machine key, fallback to value.
        k = x.get("key")
        if isinstance(k, str) and k.strip():
            return k.strip()
        v = x.get("value")
        if isinstance(v, str) and v.strip():
            return v.strip()
    # Unknown type
    return None


def fetch_product_details_by_ids(
    product_ids: List[str],
    product_family: Optional[str] = None,
    lang: str = "en",
    base_url: str = DEFAULT_PRODUCTS_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    """List-endpoint details (NOT the ITEM endpoint).

    This uses /products with filters. It's useful for debugging and for list-level fields.
    For technical details use fetch_product_item + prune_details.
    """
    filters = {"product": product_ids}

    if product_family:
        filters["product_family"] = [product_family]

    params = {"lang": lang}
    params.update(build_filter_params(filters))

    return api_get(base_url, params=params, timeout=timeout)


def search_products_raw(
    filters: dict,
    language: str = "en",
    limit: int = 10,
    base_url: str = DEFAULT_PRODUCTS_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    """Small helper used by get_product_details to resolve name -> numeric_product_id/mounting/lighting."""
    params = {
        "lang": language,
        "page[limit]": limit,
    }
    params.update(build_filter_params(filters))
    return api_get(base_url, params=params, timeout=timeout)


def fetch_product_item(
    numeric_product_id: int,
    mounting_type: Union[str, Dict[str, Any]],
    lighting_category: Union[str, Dict[str, Any]],
    language: str = "en",
    system_sku: Optional[str] = None,
    base_url: str = DEFAULT_PRODUCTS_API_URL,
    timeout=None,
) -> Dict[str, Any]:
    """ITEM endpoint: /products/{numeric_product_id}/{mounting_type}/{lighting_category}.

    mounting_type and lighting_category MUST be the machine keys (e.g. 'TRACK', 'SPOTLIGHT').
    The list endpoint may provide these as objects; we normalize to their .key here.
    """
    mt = _key_or_value(mounting_type)
    lc = _key_or_value(lighting_category)

    if not mt or not lc:
        return {
            "error": "Cannot call ITEM endpoint: mounting_type or lighting_category missing/invalid",
            "debug": {
                "numeric_product_id": numeric_product_id,
                "mounting_type": mounting_type,
                "lighting_category": lighting_category,
                "normalized_mounting_type": mt,
                "normalized_lighting_category": lc,
            },
        }

    url = f"{base_url}/{numeric_product_id}/{mt}/{lc}"
    params: Dict[str, Any] = {"lang": language}
    if system_sku:
        params["system_sku"] = system_sku
    return api_get(url, params=params, timeout=timeout)


def prune_details(response: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only the most useful blocks for LLM summarization/spec-text generation.

    IMPORTANT:
    - Tool outputs should stay compact. Returning the full raw API response can easily exceed
      model/tool output limits and make the agent unreliable in OpenWebUI.
    - If you need raw payloads for debugging, set environment variable:
        PRODUCTS_INCLUDE_RAW_DETAILS=1
    """
    data = response.get("data", [])
    trimmed = []

    for item in data:
        attrs = item.get("attributes", {})
        trimmed.append(
            {
                "product_id": attrs.get("id"),
                "is_system": bool(attrs.get("is_system", False)),
                "family_id": attrs.get("product_family_id"),
                "mounting_type": attrs.get("mounting_type"),
                "lighting_category": attrs.get("lighting_category"),
                "system_sku": derive_system_sku(attrs.get("contained_article_skus") or [])
                if bool(attrs.get("is_system", False))
                else None,
                "electrical": attrs.get("electrical"),
                "lighting": attrs.get("lighting"),
                "dimensions": attrs.get("dimensions"),
                "categorization": attrs.get("categorization"),
            }
        )

    out: Dict[str, Any] = {
        "meta": response.get("meta"),
        "data": trimmed,
    }

    if os.getenv("PRODUCTS_INCLUDE_RAW_DETAILS", "0") == "1":
        out["raw"] = response

    return out
