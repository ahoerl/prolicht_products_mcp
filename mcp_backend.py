from __future__ import annotations

import json
import os
import threading
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import requests
from requests.adapters import HTTPAdapter

try:
    # urllib3 is a dependency of requests
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover
    Retry = None  # type: ignore


DEFAULT_PRODUCTS_API_URL = os.getenv(
    "PRODUCTS_API_URL",
    "https://webapi.dev.prolicht.at/api/v1/products",
)

DEFAULT_FILTERS_API_URL = DEFAULT_PRODUCTS_API_URL + "/filters"

# Default network behavior (tunable via env)
DEFAULT_CONNECT_TIMEOUT = float(os.getenv("PRODUCTS_API_CONNECT_TIMEOUT", "5"))
DEFAULT_READ_TIMEOUT = float(os.getenv("PRODUCTS_API_READ_TIMEOUT", "60"))
DEFAULT_TIMEOUT: Tuple[float, float] = (DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT)

DEFAULT_RETRIES_TOTAL = int(os.getenv("PRODUCTS_API_RETRIES_TOTAL", "3"))
DEFAULT_RETRIES_BACKOFF = float(os.getenv("PRODUCTS_API_RETRIES_BACKOFF", "0.5"))
DEFAULT_RETRIES_STATUS = tuple(
    int(x)
    for x in os.getenv("PRODUCTS_API_RETRIES_STATUS", "429,500,502,503,504").split(",")
    if str(x).strip().isdigit()
)


# -------------------------------------------------------
# Shared normalization helpers
# -------------------------------------------------------

def key_or_value(x: Union[str, Dict[str, Any], None]) -> Optional[str]:
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


def derive_system_sku(contained_article_skus: Iterable[object]) -> Optional[str]:
    """Derive system_sku from upstream contained_article_skus.

    Rules:
    - Only consider SKUs that start with '8150-'
    - Must be a prefix match (no 'contains')
    - If multiple match, concatenate in encountered order separated by commas
    - De-duplicate while preserving order
    """
    if not contained_article_skus:
        return None

    seen = set()
    matches: List[str] = []

    for raw in contained_article_skus:
        s = str(raw).strip()
        if not s:
            continue
        if not s.startswith("8150-"):
            continue
        if s in seen:
            continue
        seen.add(s)
        matches.append(s)

    if not matches:
        return None
    return ",".join(matches)


# -------------------------------------------------------
# Filter/query helpers
# -------------------------------------------------------

def _normalize_to_list(value: Union[str, Sequence[str], None]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except Exception:
                pass
        return [s]
    return [str(v).strip() for v in value if str(v).strip()]



def _encode_filter_value(values: List[str]) -> str:
    values = [v for v in values if v]
    if not values:
        return ""
    if len(values) == 1:
        return values[0]  # single must be plain string
    return json.dumps(values, ensure_ascii=False)



def build_filter_params(filters: Dict[str, List[str]]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for key, raw_values in filters.items():
        values = _normalize_to_list(raw_values)
        if not values:
            continue
        encoded = _encode_filter_value(values)
        if encoded:
            params[f"filter[{key}]"] = encoded
    return params


# -------------------------------------------------------
# Shared requests session with retries + pooling
# -------------------------------------------------------
_session_lock = threading.Lock()
_session: Optional[requests.Session] = None



def _get_session() -> requests.Session:
    global _session
    if _session is not None:
        return _session

    with _session_lock:
        if _session is not None:
            return _session

        s = requests.Session()

        if Retry is not None and DEFAULT_RETRIES_TOTAL > 0:
            retry = Retry(
                total=DEFAULT_RETRIES_TOTAL,
                connect=DEFAULT_RETRIES_TOTAL,
                read=DEFAULT_RETRIES_TOTAL,
                status=DEFAULT_RETRIES_TOTAL,
                backoff_factor=DEFAULT_RETRIES_BACKOFF,
                status_forcelist=list(DEFAULT_RETRIES_STATUS),
                allowed_methods=frozenset(["GET"]),
                raise_on_status=False,
                respect_retry_after_header=True,
            )
            adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        else:  # pragma: no cover
            adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)

        s.mount("http://", adapter)
        s.mount("https://", adapter)

        _session = s
        return s



def _normalize_timeout(timeout: Union[int, float, Tuple[float, float], None]) -> Tuple[float, float]:
    """Convert timeout input into a (connect, read) tuple."""
    if timeout is None:
        return DEFAULT_TIMEOUT
    if isinstance(timeout, (int, float)):
        return (DEFAULT_CONNECT_TIMEOUT, float(timeout))
    try:
        c, r = timeout  # type: ignore[misc]
        return (float(c), float(r))
    except Exception:
        return DEFAULT_TIMEOUT



def api_get(
    url: str,
    params: Dict[str, Any],
    timeout: Union[int, float, Tuple[float, float], None] = None,
) -> Dict[str, Any]:
    """GET wrapper that is resilient to timeouts/transient errors.

    - Returns a dict with an 'error' key instead of raising for common network issues.
      This prevents MCP tools from failing hard on temporary slowness.
    - Non-2xx responses are surfaced as {'error': ..., 'status_code': ...}.

    Callers should treat the result as JSON:API-like response *or* an error dict.
    """

    headers = {"Accept": "application/vnd.api+json"}
    t = _normalize_timeout(timeout)

    try:
        sess = _get_session()
        resp = sess.get(url, headers=headers, params=params, timeout=t)
    except requests.exceptions.Timeout as e:
        return {
            "error": "Request timed out",
            "error_type": e.__class__.__name__,
            "url": url,
            "params": params,
            "timeout": {"connect": t[0], "read": t[1]},
        }
    except requests.exceptions.RequestException as e:
        return {
            "error": "Request failed",
            "error_type": e.__class__.__name__,
            "details": str(e),
            "url": url,
            "params": params,
            "timeout": {"connect": t[0], "read": t[1]},
        }

    if not (200 <= resp.status_code < 300):
        body_preview = None
        try:
            body_preview = resp.text[:500]
        except Exception:
            body_preview = None

        return {
            "error": "Non-success status code",
            "status_code": resp.status_code,
            "url": url,
            "params": params,
            "body_preview": body_preview,
        }

    try:
        return resp.json()
    except Exception as e:
        return {
            "error": "Failed to parse JSON response",
            "error_type": e.__class__.__name__,
            "url": url,
            "params": params,
        }
