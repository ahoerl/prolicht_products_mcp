from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

import mcp_backend_articles as articles_backend
import mcp_backend_filters as filters_backend
import mcp_backend_productdetails as details_backend
import mcp_backend_products as products_backend
from mcp_backend import derive_system_sku

logger = logging.getLogger(__name__)

RESOLVER_LANGUAGE = os.getenv("ZIP_RESOLVER_LANGUAGE", "en")
RESOLVER_PER_PAGE = int(os.getenv("ZIP_RESOLVER_PER_PAGE", "200"))
RESOLVER_MAX_TOTAL_VARIANTS = int(os.getenv("ZIP_RESOLVER_MAX_TOTAL_VARIANTS", "20000"))
RESOLVER_INCLUDE_SYSTEM_MEMBERS = os.getenv("ZIP_RESOLVER_INCLUDE_SYSTEM_MEMBERS", "1") == "1"


class ZipContextResolver:
    def __init__(self, language: str = "en") -> None:
        self.language = language
        self.sku_index: Dict[str, Dict[str, Any]] = {}
        self.stats: Dict[str, Any] = {
            "language": language,
            "families_scanned": 0,
            "variants_scanned": 0,
            "systems_scanned": 0,
            "system_inserts_scanned": 0,
            "sku_contexts_indexed": 0,
            "built_at_unix": None,
            "build_duration_seconds": None,
        }

    def _set_context(self, sku: str | None, context: Dict[str, Any], priority: int) -> None:
        key = str(sku or "").strip()
        if not key:
            return
        existing = self.sku_index.get(key)
        if existing and int(existing.get("priority", 0)) > priority:
            return

        product_name = str(context.get("product_name") or key).strip() or key
        mounting_type = str(context.get("mounting_type") or "UNKNOWN_MOUNTING").strip() or "UNKNOWN_MOUNTING"
        lighting_category = str(context.get("lighting_category") or "UNKNOWN_CATEGORY").strip() or "UNKNOWN_CATEGORY"
        system_name = context.get("system_name")
        if system_name is not None:
            system_name = str(system_name).strip() or None

        folder_parts = [product_name, mounting_type, lighting_category]
        if system_name:
            folder_parts.append(system_name)

        stored = {
            "sku": key,
            "product_name": product_name,
            "mounting_type": mounting_type,
            "lighting_category": lighting_category,
            "system_name": system_name,
            "system_sku": context.get("system_sku"),
            "product_family_id": context.get("product_family_id"),
            "is_system": bool(context.get("is_system", False)),
            "source": context.get("source"),
            "priority": priority,
            "folder_path": "/".join(folder_parts),
        }
        self.sku_index[key] = stored

    def _fetch_all_family_keys(self) -> List[str]:
        raw = filters_backend.fetch_product_filters(active_filters=None, lang=self.language)
        families_block = filters_backend.extract_product_families(raw)
        values = families_block.get("families", []) or []
        out: List[str] = []
        for v in values:
            if isinstance(v, dict):
                k = v.get("key")
                if isinstance(k, str) and k.strip():
                    out.append(k.strip())
            elif isinstance(v, str) and v.strip():
                out.append(v.strip())
        return out

    def _extract_contained_article_skus(self, raw_item: Dict[str, Any]) -> List[str]:
        data = raw_item.get("data") or []
        if not isinstance(data, list):
            return []
        out: List[str] = []
        seen = set()
        for item in data:
            if not isinstance(item, dict):
                continue
            attrs = item.get("attributes") or {}
            if not isinstance(attrs, dict):
                continue
            for raw_sku in attrs.get("contained_article_skus") or []:
                sku = str(raw_sku or "").strip()
                if not sku or sku in seen:
                    continue
                seen.add(sku)
                out.append(sku)
        return out

    def _fetch_product_item_contained_skus(
        self,
        numeric_product_id: Any,
        mounting_type: Any,
        lighting_category: Any,
        system_sku: str | None = None,
    ) -> List[str]:
        try:
            raw_item = details_backend.fetch_product_item(
                numeric_product_id=int(numeric_product_id),
                mounting_type=mounting_type,
                lighting_category=lighting_category,
                language=self.language,
                system_sku=system_sku,
            )
        except Exception:
            return []
        if not isinstance(raw_item, dict) or raw_item.get("error"):
            return []
        return self._extract_contained_article_skus(raw_item)

    def _index_regular_variant(self, variant: Dict[str, Any]) -> None:
        base_context = {
            "product_name": variant.get("product_name"),
            "mounting_type": variant.get("mounting_type"),
            "lighting_category": variant.get("lighting_category"),
            "system_name": None,
            "system_sku": variant.get("system_sku"),
            "product_family_id": variant.get("product_family_id"),
            "is_system": bool(variant.get("is_system", False)),
            "source": "family_variant",
        }

        primary_article_sku = variant.get("primary_article_sku")
        self._set_context(primary_article_sku, base_context, priority=70)

        contained_article_skus = self._fetch_product_item_contained_skus(
            numeric_product_id=variant.get("numeric_product_id"),
            mounting_type=variant.get("mounting_type"),
            lighting_category=variant.get("lighting_category"),
            system_sku=None,
        )
        for sku in contained_article_skus:
            self._set_context(sku, base_context, priority=70)

    def _index_system_variant(self, variant: Dict[str, Any]) -> None:
        system_name = str(variant.get("product_name") or "").strip() or None
        system_sku = str(variant.get("system_sku") or "").strip() or None
        if not system_sku:
            contained_article_skus = self._fetch_product_item_contained_skus(
                numeric_product_id=variant.get("numeric_product_id"),
                mounting_type=variant.get("mounting_type"),
                lighting_category=variant.get("lighting_category"),
                system_sku=None,
            )
            system_sku = derive_system_sku(contained_article_skus)
        if not system_sku:
            return

        self.stats["systems_scanned"] = int(self.stats.get("systems_scanned", 0)) + 1
        system_context = {
            "product_name": variant.get("product_name"),
            "mounting_type": variant.get("mounting_type"),
            "lighting_category": variant.get("lighting_category"),
            "system_name": system_name,
            "system_sku": system_sku,
            "product_family_id": variant.get("product_family_id"),
            "is_system": True,
            "source": "system_variant",
        }
        self._set_context(system_sku, system_context, priority=60)
        primary_article_sku = variant.get("primary_article_sku")
        self._set_context(primary_article_sku, system_context, priority=60)

        offset = 0
        while True:
            raw = products_backend.search_products(
                filters={"system_sku": [system_sku]},
                lang=self.language,
                limit=RESOLVER_PER_PAGE,
                offset=offset,
            )
            pruned = products_backend.prune_product_list(raw)
            results = pruned.get("results") or []
            if not isinstance(results, list) or not results:
                break

            for insert_variant in results:
                if not isinstance(insert_variant, dict):
                    continue
                self.stats["system_inserts_scanned"] = int(self.stats.get("system_inserts_scanned", 0)) + 1
                insert_context = {
                    "product_name": insert_variant.get("product_name"),
                    "mounting_type": insert_variant.get("mounting_type"),
                    "lighting_category": insert_variant.get("lighting_category"),
                    "system_name": system_name,
                    "system_sku": system_sku,
                    "product_family_id": insert_variant.get("product_family_id") or variant.get("product_family_id"),
                    "is_system": False,
                    "source": "system_insert_variant",
                }
                self._set_context(insert_variant.get("primary_article_sku"), insert_context, priority=100)
                contained_article_skus = self._fetch_product_item_contained_skus(
                    numeric_product_id=insert_variant.get("numeric_product_id"),
                    mounting_type=insert_variant.get("mounting_type"),
                    lighting_category=insert_variant.get("lighting_category"),
                    system_sku=system_sku,
                )
                for sku in contained_article_skus:
                    self._set_context(sku, insert_context, priority=100)

            returned = len(results)
            limit = int(pruned.get("meta", {}).get("limit") or RESOLVER_PER_PAGE)
            if returned < limit:
                break
            offset += limit

        if RESOLVER_INCLUDE_SYSTEM_MEMBERS:
            try:
                members = articles_backend.get_system_member_articles(system_sku=system_sku, language=self.language)
            except Exception:
                members = {}
            if isinstance(members, dict):
                data = members.get("data")
                if isinstance(data, list):
                    for item in data:
                        if not isinstance(item, dict):
                            continue
                        attrs = item.get("attributes") or {}
                        if not isinstance(attrs, dict):
                            continue
                        member_sku = str(attrs.get("sku") or attrs.get("id") or "").strip()
                        if member_sku and member_sku not in self.sku_index:
                            self._set_context(member_sku, system_context, priority=50)

    def build(self) -> "ZipContextResolver":
        started = time.time()
        family_keys = self._fetch_all_family_keys()
        self.stats["families_scanned"] = len(family_keys)
        total_variants_seen = 0

        for family_key in family_keys:
            offset = 0
            while total_variants_seen < RESOLVER_MAX_TOTAL_VARIANTS:
                raw = products_backend.search_products(
                    filters={"product_family": [family_key]},
                    lang=self.language,
                    limit=RESOLVER_PER_PAGE,
                    offset=offset,
                )
                pruned = products_backend.prune_product_list(raw)
                results = pruned.get("results") or []
                if not isinstance(results, list) or not results:
                    break

                for variant in results:
                    if not isinstance(variant, dict):
                        continue
                    total_variants_seen += 1
                    if total_variants_seen > RESOLVER_MAX_TOTAL_VARIANTS:
                        break
                    self.stats["variants_scanned"] = int(self.stats.get("variants_scanned", 0)) + 1
                    if bool(variant.get("is_system", False)):
                        self._index_system_variant(variant)
                    else:
                        self._index_regular_variant(variant)

                returned = len(results)
                limit = int(pruned.get("meta", {}).get("limit") or RESOLVER_PER_PAGE)
                if returned < limit or total_variants_seen >= RESOLVER_MAX_TOTAL_VARIANTS:
                    break
                offset += limit

        self.stats["sku_contexts_indexed"] = len(self.sku_index)
        self.stats["built_at_unix"] = int(time.time())
        self.stats["build_duration_seconds"] = round(time.time() - started, 3)
        return self

    def get_context(self, sku: str) -> Dict[str, Any]:
        key = str(sku or "").strip()
        if not key:
            return {
                "sku": key,
                "product_name": None,
                "mounting_type": "UNKNOWN_MOUNTING",
                "lighting_category": "UNKNOWN_CATEGORY",
                "system_name": None,
                "system_sku": None,
                "folder_path": "UNKNOWN_PRODUCT/UNKNOWN_MOUNTING/UNKNOWN_CATEGORY",
                "source": "empty_sku",
            }
        if key in self.sku_index:
            return dict(self.sku_index[key])
        return {
            "sku": key,
            "product_name": key,
            "mounting_type": "UNKNOWN_MOUNTING",
            "lighting_category": "UNKNOWN_CATEGORY",
            "system_name": None,
            "system_sku": None,
            "folder_path": f"{key}/UNKNOWN_MOUNTING/UNKNOWN_CATEGORY",
            "source": "resolver_fallback",
        }


_resolver_lock = threading.Lock()
_resolver: Optional[ZipContextResolver] = None
_resolver_error: Optional[str] = None


def initialize_zip_context_resolver(force_rebuild: bool = False, language: str = RESOLVER_LANGUAGE) -> ZipContextResolver:
    global _resolver, _resolver_error
    with _resolver_lock:
        if _resolver is not None and not force_rebuild:
            return _resolver
        resolver = ZipContextResolver(language=language)
        try:
            resolver.build()
            _resolver = resolver
            _resolver_error = None
            logger.info("ZIP context resolver ready: %s", resolver.stats)
            return resolver
        except Exception as exc:
            _resolver_error = f"{exc.__class__.__name__}: {exc}"
            logger.exception("ZIP context resolver build failed")
            raise


def ensure_zip_context_resolver(language: str = RESOLVER_LANGUAGE) -> ZipContextResolver:
    global _resolver
    if _resolver is not None:
        return _resolver
    return initialize_zip_context_resolver(force_rebuild=False, language=language)


def get_zip_context_for_sku(sku: str, language: str = RESOLVER_LANGUAGE) -> Dict[str, Any]:
    resolver = ensure_zip_context_resolver(language=language)
    return resolver.get_context(sku)


def get_zip_context_resolver_status() -> Dict[str, Any]:
    return {
        "ready": _resolver is not None,
        "error": _resolver_error,
        "stats": dict(_resolver.stats) if _resolver is not None else None,
    }
