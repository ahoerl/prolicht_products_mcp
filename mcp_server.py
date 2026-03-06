from __future__ import annotations
import os
import uvicorn

from fastmcp import FastMCP
from fastmcp.server.http import create_streamable_http_app

# Annotated + Field are used to enrich the JSON schema that the LLM sees for each tool.
# This massively improves tool selection and parameter correctness in OpenWebUI.
from typing import Annotated
from pydantic import Field

import mcp_backend_filters as filters_backend
import mcp_backend_products as products_backend
import mcp_backend_productdetails as details_backend


import mcp_backend_families as families_backend
import mcp_backend_system as system_backend
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8102"))

mcp = FastMCP("Products MCP v1")



# Gemeinsame Typ-Aliase für bessere JSON-Schemas
# NOTE FOR LLMs (important):
# - Use ONLY filter IDs and option keys returned by get_product_search_options.
# - Do NOT invent filter names (e.g. don't guess "recessed" vs "RECESSED").
# - Values must be the machine keys (values[].key), not the human label.
FilterDict = Annotated[
    dict[str, list[str]],
    Field(
        description=(
            "Product search filters. Keys MUST be valid filter IDs from get_product_search_options "
            "(e.g. 'product_family', 'product', 'mounting_type', 'product_categories', "
            "'segments', 'applications', 'color_temperatures', 'ip_rates', 'dimmability', ...). "
            "Values MUST be lists of allowed option keys (values[].key) from get_product_search_options.\n\n"
            "Rule of thumb: if the user mentions a filter in free text (e.g. \"Büro\", \"Office\", \"Workplace\", "
            "\"Einbau\", \"Anbau\", \"IP20\", \"3000K\"), call get_product_search_options() first to get "
            "the exact filter key + option key, then call search_products.\n\n"
            "Common DE mappings (use option keys from get_product_search_options):\n"
            "  Büro / Office -> {'segments': ['Office']}\n"
            "  Anwendung (z.B. Workplace) -> {'applications': ['Workplace']}\n"
            "  Einbau -> {'mounting_type': ['RECESSED']}\n"
            "  Anbau -> {'mounting_type': ['SURFACE_MOUNTED']}\n"
            "  Pendel -> {'mounting_type': ['PENDANT']}\n\n"
            "Examples:\n"
            "  {'segments': ['Office'], 'mounting_type': ['RECESSED']}\n"
            "  {'product_categories': ['DOWNLIGHT'], 'ip_rates': ['IP20']}\n"
            "  {'product_family': ['MAGIQ', 'INVADER']}\n"
            "  {'system_sku': ['8150-094']}  # fetch inserts/components by system_sku"
)
    ),
]

Language = Annotated[
    str,
    Field(
        description=(
            "Response language forwarded to the API (query param 'lang'). "
            "Use 'en' or 'de'. Default: 'en'."
        )
    ),
]

ActiveFilters = Annotated[
    dict[str, list[str]] | None,
    Field(
        description=(
            "Optional faceted narrowing: same structure as `filters` in search_products. "
            "If provided, the API returns filter options that remain valid within this selection.\n\n"
            "Use this to progressively narrow filters:\n"
            "  1) call get_product_search_options() to see all filters\n"
            "  2) set active_filters based on user intent\n"
            "  3) call get_product_search_options(active_filters=...) to get remaining valid options\n\n"
            "IMPORTANT: active_filters should use ONLY keys/values returned by get_product_search_options."
        )
    ),
]

Limit = Annotated[
    int,
    Field(
        ge=1,
        le=200,
        description="Page size for search_products (API page[limit]). Default: 50.",
    ),
]

ProductName = Annotated[
    str,
    Field(
        description=(
            "Product display name as seen in the product list (e.g. 'INVADER ADJUSTABLE'). "
            "This is not a SKU. The tool will resolve it to numeric_product_id + mounting_type + lighting_category."
        )
    ),
]









MountingTypeKey = Annotated[
    str | None,
    Field(
        description=(
            "Optional mounting type machine key used to disambiguate variants when a product name exists in multiple mountings. "
            "Use ONLY keys like 'RECESSED', 'SURFACE_MOUNTED', 'PENDANT', 'TRACK' (never human labels)."
        )
    ),
]

LightingCategoryKey = Annotated[
    str | None,
    Field(
        description=(
            "Optional lighting category machine key used to disambiguate variants when a product name exists in multiple categories. "
            "Use ONLY keys like 'DOWNLIGHT', 'SPOTLIGHT', 'WALL_WASHER', ... (never human labels)."
        )
    ),
]

NumericProductId = Annotated[
    int | None,
    Field(
        ge=1,
        description=(
            "Optional numeric_product_id from search_products results. "
            "Strongest disambiguator when product_name exists in multiple variants."
        ),
    ),
]

SystemSku = Annotated[
    str | None,
    Field(
        description=(
            "Optional system SKU context (e.g. '8150-094'). "
            "Required when fetching ITEM details for a system insert/component that belongs to a system. "
            "If provided, it is passed as query parameter `system_sku` to the ITEM endpoint."
        )
    ),
]

# -------------------------------------------------
# Tools
# -------------------------------------------------

@mcp.tool
def list_product_families(language: Language = "en"):
    """
    Returns ALL available product families.
    Use this before filtering if unsure about valid family keys.
    """
    raw = filters_backend.fetch_product_filters(lang=language)
    return filters_backend.extract_product_families(raw)


@mcp.tool
def get_product_search_options(active_filters: ActiveFilters = None, language: Language = "en"):
    """
    Search helper: returns VALID filter IDs and allowed values for product search.

    What you get:
    - For each filter (e.g. product_family, product, mounting_type, product_categories, segments, applications, ...),
      you receive allowed option keys (values[].key) that can be used in search_products.

    When to use:
    - BEFORE search_products if the user mentions filters in free text (e.g. "Büro/Office", "Workplace", "Einbau", "IP20", "3000K").
    - If you are unsure which filter IDs exist or which option keys are valid.

    How to use it:
    - Map user intent to filter IDs and option keys from this response (use ONLY values[].key).
    - Then call search_products(filters={...}) using ONLY those keys.

    Important:
    - If active_filters is omitted: returns ALL possible options.
    - If active_filters is provided: returns options remaining within that filtered result set (faceted filtering).

    LLM Guidance:
    - segments/applications are marketing filters (e.g. Büro -> segments=['Office']).
    - For questions like "welche SIGN gibt es in Anbau" or "Produkte fürs Büro":
      1) get_product_search_options(language='de')
      2) search_products(filters={'product_family':['SIGN'], 'mounting_type':['SURFACE_MOUNTED']}, language='de')
         or search_products(filters={'segments':['Office']}, language='de')

Important:
- This tool ONLY returns filter OPTIONS (valid keys and allowed values). It does NOT return products.
- To get an actual product list, ALWAYS follow with search_products(filters=...).

Two-step process for natural language queries:
1) Call get_product_search_options(language=...) to identify the correct filter key + value keys.
2) Call search_products(filters={...}, language=...) to retrieve matching products.

Example (Kindergarten):
- get_product_search_options(language="de")
- search_products(filters={"applications":["SchoolKindergardenUniversity"], "segments":["Education"]}, language="de")
"""
    raw = filters_backend.fetch_product_filters(active_filters, language)
    llm_usage = {
        "rule": "Use only option keys (values[].key) from `filters` as filter values in search_products. Do not invent keys or values.",
        "two_step_process": [
            "1) Call get_product_search_options(language=...) to get valid filter keys and values[].key.",
            "2) Call search_products(filters={...}) using ONLY those keys to retrieve products."
        ],
        "must_run_search_products_for_lists": "If the user asks for a product list (e.g. segment/application like Retail/Office/Kindergarten), you MUST call search_products and return results. Do not reply that only options exist.",
        "example_retail": {"filters": {"segments": ["Retail"]}},
        "example_kindergarten": {"filters": {"applications": ["SchoolKindergardenUniversity"], "segments": ["Education"]}},
        "de_synonyms": {
            "Büro": {"segments": ["Office"]},
            "Office": {"segments": ["Office"]},
            "Anbau": {"mounting_type": ["SURFACE_MOUNTED"]},
            "Einbau": {"mounting_type": ["RECESSED"]},
            "Pendel": {"mounting_type": ["PENDANT"]},
        },
        "common_recipes": [
            {"intent": "Produkte fürs Büro", "filters": {"segments": ["Office"]}},
            {"intent": "Büro + Einbau", "filters": {"segments": ["Office"], "mounting_type": ["RECESSED"]}},
            {"intent": "Produkte fürs Segment Retail", "filters": {"segments": ["Retail"]}},
            {"intent": "Kindergarten/Schule", "filters": {"applications": ["SchoolKindergardenUniversity"], "segments": ["Education"]}},
        ],
    }
    return {"llm_usage": llm_usage, "filters": raw}


@mcp.tool
def search_products(
    filters: FilterDict = Field(default_factory=dict),
    language: Language = "en",
    limit: Limit = 50,
    include_raw: Annotated[
        bool,
        Field(
            description=(
                "If true, returns both a compact LLM-friendly result AND the full raw API payload. "
                "Default is false to avoid huge outputs that can overwhelm the model."
            )
        ),
    ] = False,
):
    """
    Search products using filter keys from get_product_search_options.

    Returns:
    - By default (include_raw=False): a PRUNED list with the most important fields per product variant.
      This keeps responses small and reliable for LLMs.
    - If include_raw=True: includes the original API JSON in addition to the pruned list (debug only).

    LLM Guidance:
    - If you are unsure which filter keys/values are valid, call get_product_search_options first.
    - Do not guess filter keys or option keys.
    - Use get_product_details to fetch the ITEM endpoint for full technical details of one product.

Critical note (MUST follow):
- If the user asks for a PRODUCT LIST using marketing intent (segment/application) or any other filter
  (e.g. "alle Produkte fürs Segment Retail", "für Kindergarten", "für Office/Büro", "für Hotel", "Anwendung Workplace"),
  you MUST execute a product search and return the results from this tool.
- Do NOT answer with "no concrete product list available" if the API can be queried.
- Do NOT ask follow-up questions like mounting type unless the user explicitly wants a narrower subset.
  Default behavior: search with the given filters and return all matching products (then offer optional refinement).
- Two-step rule:
  1) If you are unsure about the correct filter key/value, call get_product_search_options(language=...) to confirm.
  2) Then call search_products(filters={...}).
- Shortcut:
  - If the user explicitly names an option key/value that is obviously valid (e.g. "Retail", "Office", "Education"),
    you may call search_products directly (still prefer keys from get_product_search_options when in doubt).


"""
    raw = products_backend.search_products(
        filters=filters,
        lang=language,
        limit=limit,
    )
    pruned = products_backend.prune_product_list(raw)

    # Group variants by product_name so the chat layer can list available mountings/categories
    # without guessing or "mixing" variants across products.
    grouped_map: dict[str, dict[str, object]] = {}
    for r in (pruned.get("results") or []):
        name = (r.get("product_name") or "").strip()
        if not name:
            continue
        g = grouped_map.setdefault(
            name,
            {"product_name": name, "mounting_types": set(), "lighting_categories": set(), "variant_count": 0},
        )
        mt = (r.get("mounting_type") or "").strip()
        lc = (r.get("lighting_category") or "").strip()
        if mt:
            g["mounting_types"].add(mt)  # type: ignore[attr-defined]
        if lc:
            g["lighting_categories"].add(lc)  # type: ignore[attr-defined]
        g["variant_count"] = int(g.get("variant_count", 0)) + 1  # type: ignore[arg-type]

    grouped_by_product_name = []
    for name, g in sorted(grouped_map.items(), key=lambda kv: kv[0]):
        grouped_by_product_name.append(
            {
                "product_name": name,
                "variant_count": g["variant_count"],
                "mounting_types": sorted(list(g["mounting_types"])),  # type: ignore[arg-type]
                "lighting_categories": sorted(list(g["lighting_categories"])),  # type: ignore[arg-type]
            }
        )

    response = {
        "query": {"filters": filters, "language": language, "limit": limit},
        "meta": raw.get("meta", {}) if isinstance(raw, dict) else {},
        "results": pruned,
        "grouped_by_product_name": grouped_by_product_name,
    }

    if include_raw:
        response["raw"] = raw
    return response


@mcp.tool
def get_system_inserts(
    product_family_id: Annotated[
        str,
        Field(description="Product family key, e.g. 'HYPRO'."),
    ],
    product_name: Annotated[
        str,
        Field(description="System product display name (attributes.id), e.g. 'HYPRO 40'."),
    ],
    mounting_type: Annotated[
        str,
        Field(description="Mounting type machine key, e.g. 'RECESSED', 'PENDANT', 'SURFACE_MOUNTED'."),
    ],
    language: Language = "en",
    limit: Limit = 200,
):
    """Get inserts/components for a system variant WITHOUT requiring the user to provide a SKU.

    This tool resolves the system_sku for the requested system variant (family + product name + mounting type)
    and then calls /products with filter[system_sku]=... to list the inserts/components.

    Returns a pruned list with aggregated technical ranges per insert.
    """
    return system_backend.get_system_inserts(
        product_family_id=product_family_id,
        product_name=product_name,
        mounting_type=mounting_type,
        language=language,
        limit=limit,
    )

@mcp.tool
def get_products_grouped_by_family(
    family_ids: Annotated[
        list[str] | None,
        Field(
            description=(
                "Optional list of product family keys (e.g. ['MAGIQ','INVADER']). "
                "If omitted or null, the tool will fetch all families via /products/filters "
                "and then aggregate products for each family."
            )
        ),
    ] = None,
    language: Language = "en",
    per_page: Annotated[
        int,
        Field(
            ge=1,
            le=200,
            description=(
                "Internal page size for API pagination (page[limit]). "
                "Higher values reduce requests but increase payload size. Default: 50."
            ),
        ),
    ] = 50,
    max_variants_per_family: Annotated[
        int,
        Field(
            ge=1,
            le=5000,
            description=(
                "Safety limit: maximum number of product variants to return per family. "
                "Prevents overly large responses. Default: 500."
            ),
        ),
    ] = 500,
    max_total_variants: Annotated[
        int,
        Field(
            ge=1,
            le=20000,
            description=(
                "Safety limit: maximum number of product variants to return across ALL families. "
                "Prevents overly large responses. Default: 3000."
            ),
        ),
    ] = 3000,
    include_raw: Annotated[
        bool,
        Field(
            description=(
                "If true, also returns raw page payloads for debugging. "
                "Default false (raw payloads can be huge)."
            )
        ),
    ] = False,
):
    """
    Build a deterministic mapping: product_family -> product variants, using ONLY the WebAPI.

    Why this exists:
    - /products/filters returns only the family keys (filter options), not the contained products.
    - For 'family -> products' answers we must query /products and group by product_family_id.

    Output is compact/pruned per variant (to avoid 100k+ character payloads):
    - product_name, numeric_product_id, product_family_id
    - mounting_type (key), lighting_category (key)
    - is_system (if present)
    - contained_article_skus ONLY when is_system==true (needed to fetch inserts later)
    """
    return families_backend.get_products_grouped_by_family(
        family_ids=family_ids,
        language=language,
        per_page=per_page,
        max_variants_per_family=max_variants_per_family,
        max_total_variants=max_total_variants,
        include_raw=include_raw,
    )


@mcp.tool
def get_products_details_by_ids(
    product_ids: Annotated[
        list[str],
        Field(
            description=(
                "Advanced/debug: list of product IDs as used by the /products list endpoint "
                "(not the ITEM endpoint). Prefer get_product_details for end-user requests."
            )
        ),
    ],
    product_family: Annotated[
        str | None,
        Field(description="Optional product family key for narrowing (rarely needed)."),
    ] = None,
    language: Language = "en",
):
    """
    Advanced: fetch raw product details by explicit IDs (list endpoint).
    Note: This does NOT call the ITEM endpoint. For end-user technical details use get_product_details.
    """
    return details_backend.fetch_product_details_by_ids(
        product_ids=product_ids,
        product_family=product_family,
        lang=language,
    )



@mcp.tool
def resolve_product_variants(
    product_name: ProductName,
    language: Language = "en",
    system_sku: SystemSku = None,
    limit: Limit = 50,
    include_raw: Annotated[
        bool,
        Field(
            description=(
                "If true, also return the raw /products response for debugging. "
                "Default false."
            )
        ),
    ] = False,
):
    """Resolve all list-endpoint variants for a given product_name.

    Use this when the same product_name can exist in multiple mounting types / lighting categories
    and you need to confirm whether a specific variant (e.g. SURFACE_MOUNTED) exists *before*
    calling get_product_details().
    """

    base_filters: dict[str, list[str]] = {}
    if system_sku:
        base_filters["system_sku"] = [system_sku]

    filters_exact = dict(base_filters)
    filters_exact["product"] = [product_name]
    raw = details_backend.search_products_raw(filters=filters_exact, language=language, limit=limit)

    data = raw.get("data", []) if isinstance(raw, dict) else []
    if not data:
        filters_term = dict(base_filters)
        filters_term["term"] = [product_name]
        raw = details_backend.search_products_raw(filters=filters_term, language=language, limit=max(limit, 20))

    pruned = products_backend.prune_product_list(raw)
    pruned_results = pruned.get("results", []) or []

    # Prefer exact (case-insensitive) id matches if term search returned broader results.
    exact = [r for r in pruned_results if (r.get("product_name") or "").lower() == product_name.lower()]
    if exact:
        pruned_results = exact

    variants_map: dict[tuple, dict[str, object]] = {}
    for r in pruned_results:
        key = (
            r.get("numeric_product_id"),
            r.get("mounting_type"),
            r.get("lighting_category"),
            bool(r.get("is_system", False)),
            r.get("product_family_id"),
        )
        variants_map.setdefault(
            key,
            {
                "numeric_product_id": r.get("numeric_product_id"),
                "mounting_type": r.get("mounting_type"),
                "lighting_category": r.get("lighting_category"),
                "is_system": bool(r.get("is_system", False)),
                "product_family_id": r.get("product_family_id"),
            },
        )

    out = {
        "query": {"product_name": product_name, "language": language, "system_sku": system_sku, "limit": limit},
        "variants": list(variants_map.values()),
        "pruned": {"meta": pruned.get("meta"), "results": pruned_results},
    }
    if include_raw:
        out["raw"] = raw
    return out




@mcp.tool
def get_product_details(
    product_name: ProductName,
    numeric_product_id: NumericProductId = None,
    mounting_type: MountingTypeKey = None,
    lighting_category: LightingCategoryKey = None,
    language: Language = "en",
    system_sku: SystemSku = None,
):
    """Returns full technical details for a product (ITEM endpoint).

    This tool is VARIANT-AWARE:
    - If a product_name exists in multiple mounting types / lighting categories, you can (and should)
      pass mounting_type and/or lighting_category and/or numeric_product_id (from search_products)
      to fetch details for the intended variant.
    - If the selection is ambiguous, the tool returns a structured list of available variants
      instead of guessing.

    Flow:
      1) Search /products (LIST endpoint) by product name (and optional system_sku context)
      2) Select the correct variant (numeric_product_id + mounting_type + lighting_category)
      3) Call the ITEM endpoint:
         /products/{numeric_product_id}/{mounting_type}/{lighting_category}
         If system_sku is provided, it is passed as query param ?system_sku=... (required for system inserts).
      4) Return pruned technical details
    """

    def _norm_key(s: str | None) -> str | None:
        if s is None:
            return None
        ss = str(s).strip()
        return ss.upper() if ss else None

    def _as_int(x) -> int | None:
        try:
            return int(x)
        except Exception:
            return None

    base_filters: dict[str, list[str]] = {}
    if system_sku:
        base_filters["system_sku"] = [system_sku]

    # Prefer exact product filter first (when product_name is a valid option key),
    # otherwise fall back to fuzzy term search.
    filters_exact = dict(base_filters)
    filters_exact["product"] = [product_name]
    search_result = details_backend.search_products_raw(
        filters=filters_exact,
        language=language,
        limit=50,
    )

    data = search_result.get("data", []) if isinstance(search_result, dict) else []
    if not data:
        filters_term = dict(base_filters)
        filters_term["term"] = [product_name]
        search_result = details_backend.search_products_raw(
            filters=filters_term,
            language=language,
            limit=50,
        )
        data = search_result.get("data", []) if isinstance(search_result, dict) else []

    if not data:
        return {
            "error": f"No product found for '{product_name}'",
            "hint": "Call search_products with broader filters (or resolve_product_variants) to verify available product names/variants. If this is a system insert, pass system_sku.",
        }

    # Prefer exact match on attributes.id (case-insensitive) when term search returned broader results.
    exact_matches = []
    for item in data:
        attrs = item.get("attributes", {}) or {}
        if str(attrs.get("id") or "").strip().lower() == product_name.strip().lower():
            exact_matches.append(item)

    candidates = exact_matches or list(data)

    # Build availability list (for helpful errors / ambiguity).
    available_variants = []
    for item in candidates:
        attrs = item.get("attributes", {}) or {}
        available_variants.append(
            {
                "numeric_product_id": attrs.get("numeric_product_id"),
                "mounting_type": details_backend._key_or_value(attrs.get("mounting_type")),
                "lighting_category": details_backend._key_or_value(attrs.get("lighting_category")),
                "is_system": bool(attrs.get("is_system", False)),
                "product_family_id": attrs.get("product_family_id"),
            }
        )

    # Apply disambiguation filters (if provided).
    wanted_np = _as_int(numeric_product_id) if numeric_product_id is not None else None
    wanted_mt = _norm_key(mounting_type)
    wanted_lc = _norm_key(lighting_category)

    filtered = []
    for item in candidates:
        attrs = item.get("attributes", {}) or {}

        if wanted_np is not None:
            if _as_int(attrs.get("numeric_product_id")) != wanted_np:
                continue

        if wanted_mt is not None:
            mt_key = _norm_key(details_backend._key_or_value(attrs.get("mounting_type")))
            if mt_key != wanted_mt:
                continue

        if wanted_lc is not None:
            lc_key = _norm_key(details_backend._key_or_value(attrs.get("lighting_category")))
            if lc_key != wanted_lc:
                continue

        filtered.append(item)

    if not filtered:
        return {
            "error": "No matching variant found for the requested constraints.",
            "debug": {
                "product_name": product_name,
                "numeric_product_id": numeric_product_id,
                "mounting_type": mounting_type,
                "lighting_category": lighting_category,
                "system_sku": system_sku,
            },
            "available_variants": available_variants,
            "hint": "Use resolve_product_variants(product_name=...) or search_products(...) to find the correct numeric_product_id/mounting_type/lighting_category, then call get_product_details again with those values.",
        }

    if len(filtered) > 1:
        return {
            "error": "Ambiguous product selection: multiple variants match. Please provide more constraints.",
            "debug": {
                "product_name": product_name,
                "numeric_product_id": numeric_product_id,
                "mounting_type": mounting_type,
                "lighting_category": lighting_category,
                "system_sku": system_sku,
                "matches": len(filtered),
            },
            "available_variants": available_variants,
            "hint": "Provide numeric_product_id (preferred) and/or mounting_type and lighting_category to disambiguate.",
        }

    selected = filtered[0].get("attributes", {}) or {}

    numeric_id = selected.get("numeric_product_id")
    mt = selected.get("mounting_type")
    lc = selected.get("lighting_category")

    if not numeric_id:
        return {"error": "numeric_product_id missing in product response", "available_variants": available_variants}
    if not mt:
        return {"error": "mounting_type missing in product response", "available_variants": available_variants}
    if not lc:
        return {"error": "lighting_category missing in product response", "available_variants": available_variants}

    raw_details = details_backend.fetch_product_item(
        numeric_product_id=numeric_id,
        mounting_type=mt,
        lighting_category=lc,
        language=language,
        system_sku=system_sku,
    )

    return details_backend.prune_details(raw_details)


# -------------------------------------------------
# Transport
# -------------------------------------------------

app = create_streamable_http_app(server=mcp, streamable_http_path="/mcp")
# -------------------------------------------------
# Debug logging (optional)
# -------------------------------------------------
# These logs help diagnose failures that happen BEFORE tool code runs
# (e.g., Pydantic validation errors when OpenWebUI calls a tool with
# missing / misnamed arguments).
#
# Enable by setting:
#   MCP_LOG_HTTP_REQUESTS=1            (logs raw POST /mcp JSON body)
#   MCP_LOG_HTTP_MAX_BYTES=10000       (optional truncation; default 10000)
#
# NOTE: Raw request bodies can contain user prompts/chat content. Use temporarily.
if os.getenv("MCP_LOG_HTTP_REQUESTS", "0") == "1":
    try:
        import logging, json
        from starlette.requests import Request

        _mcp_http_logger = logging.getLogger("mcp.http")

        @app.middleware("http")  # type: ignore[misc]
        async def _log_mcp_http_request(request: Request, call_next):
            if request.url.path == "/mcp" and request.method.upper() == "POST":
                body = await request.body()
                max_bytes = int(os.getenv("MCP_LOG_HTTP_MAX_BYTES", "10000"))
                snippet = body[:max_bytes]
                try:
                    text = snippet.decode("utf-8", "replace")
                except Exception:
                    text = repr(snippet)

                # Best-effort: extract tool name + args from MCP JSON-RPC payload (if present)
                tool_info = ""
                try:
                    payload = json.loads(body.decode("utf-8", "replace"))
                    # MCP clients vary; handle dict + list payloads
                    def _extract(p):
                        if isinstance(p, dict):
                            params = p.get("params") or {}
                            name = params.get("name") or params.get("tool") or ""
                            args = params.get("arguments") or params.get("args") or {}
                            method = p.get("method") or ""
                            if name:
                                return f" method={method} tool={name} args_keys={list(args.keys())}"
                        return ""
                    if isinstance(payload, list):
                        for p in payload:
                            ti = _extract(p)
                            if ti:
                                tool_info = ti
                                break
                    else:
                        tool_info = _extract(payload)
                except Exception:
                    pass

                _mcp_http_logger.info("POST /mcp body (truncated %s bytes)%s: %s", len(snippet), tool_info, text)

                # Re-inject body so downstream handlers can still read it
                async def receive():
                    return {"type": "http.request", "body": body, "more_body": False}
                request = Request(request.scope, receive)

                return await call_next(request)

            return await call_next(request)

    except Exception as _e:
        # Never break the server because of debug logging
        pass


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
