
import os
import json
import base64
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor
from fastapi.responses import StreamingResponse
from fastapi import Response
import sys, os
import time
from threading import Lock

import io, csv
import requests
from requests.adapters import HTTPAdapter
from fastapi import FastAPI, HTTPException, APIRouter, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv

_EXPORT_TICKETS: dict[str, tuple[float, bytes]] = {}
_EXPORT_LOCK = Lock()
_EXPORT_TTL = 600  # seconds
_CACHE: dict[str, tuple[float, Any]] = {}
_CACHE_LOCK = Lock()

HERE = os.path.abspath(os.path.dirname(__file__))
UI_DIR = os.path.join(HERE, "ui")

# Load environment
load_dotenv(os.path.join(HERE, ".env"), override=False)
load_dotenv(override=False)

LATE_FEE_THRESHOLD = float(os.getenv("LATE_FEE_THRESHOLD", "500"))
LATE_FEE_PERCENT   = float(os.getenv("LATE_FEE_PERCENT",   "0.05"))  # 5% as 0.05
LATE_FEE_BASE_FEE  = float(os.getenv("LATE_FEE_BASE_FEE",  "10"))

PROPERTY_GROUP_A = {
    "8006","8007","8008","8009","8010",
    "9099","9144","9148","9164","9169","9173","9212","9232","9250","9308",
    "9314","9327","9331","9337","9341","9345","9351","9356","9359","9370",
    "9374","9378","9384","9388","9392","9396","9429","9432","9467","9502",
    "9503","9504","9505","9506","9507","9508","9509","9510","9511","9512",
    "9513","9514","9515","9516","9517","9525","9530","9531","9538","9544"
}  # 1000 / 5% / 10
PROPERTY_GROUP_B = {
    "8011","8012","8013","8014","8015","9084","9085","9086","9087","9088",
    "9089","9090","9091","9092","9093","9094","9095","9096","9097","9098",
    "9100","9101","9102","9103","9104","9105","9106","9107","9108","9109",
    "9110","9111","9112","9113","9114","9115","9116","9117","9118","9119",
    "9120","9121","9122","9123","9124","9125","9126","9127","9128","9129",
    "9130","9131","9132","9133","9134","9135","9136","9137","9138","9139",
    "9140","9141","9142","9143","9145","9146","9147","9149","9150","9151",
    "9152","9153","9154","9155","9156","9157","9158","9159","9160","9161",
    "9162","9163","9165","9166","9167","9168","9170","9171","9172","9174",
    "9175","9176","9177","9178","9179","9180","9181","9182","9183","9184",
    "9185","9186","9187","9188","9189","9190","9191","9192","9193","9194",
    "9195","9196","9197","9198","9199","9200","9201","9202","9203","9204",
    "9205","9206","9207","9208","9209","9210","9211","9213","9214","9215",
    "9216","9217","9218","9219","9220","9221","9222","9223","9224","9225",
    "9226","9227","9228","9229","9230","9231","9233","9234","9235","9236",
    "9237","9238","9239","9240","9241","9242","9243","9244","9245","9246",
    "9247","9248","9249","9251","9252","9253","9254","9255","9256","9257",
    "9258","9259","9260","9261","9262","9263","9264","9265","9266","9267",
    "9268","9269","9270","9271","9272","9273","9274","9275","9276","9277",
    "9278","9279","9280","9281","9282","9283","9284","9285","9286","9287",
    "9288","9289","9290","9291","9292","9293","9294","9295","9296","9297",
    "9298","9299","9300","9301","9302","9303","9304","9305","9306","9307",
    "9309","9310","9311","9312","9313","9315","9316","9317","9318","9319",
    "9320","9321","9322","9323","9324","9325","9326","9328","9329","9330",
    "9332","9333","9334","9335","9336","9338","9339","9340","9342","9343",
    "9344","9346","9347","9348","9349","9350","9352","9353","9354","9355",
    "9357","9358","9360","9361","9362","9363","9364","9365","9366","9367",
    "9368","9369","9371","9372","9373","9375","9376","9377","9379","9380",
    "9381","9382","9383","9385","9386","9387","9389","9390","9391","9393",
    "9394","9395","9397","9398","9399","9400","9401","9402","9403","9404",
    "9405","9406","9407","9408","9409","9410","9411","9412","9413","9414",
    "9415","9416","9417","9418","9419","9420","9421","9422","9423","9424",
    "9425","9426","9427","9428","9430","9431","9433","9434","9435","9436",
    "9437","9438","9439","9440","9441","9442","9443","9444","9445","9446",
    "9447","9448","9449","9450","9451","9452","9453","9454","9455","9456",
    "9457","9458","9459","9460","9461","9462","9463","9464","9465","9466",
    "9468","9469","9470","9471","9472","9473","9474","9475","9476","9477",
    "9478","9479","9480","9481","9482","9483","9484","9485","9486","9487",
    "9488","9489","9490","9491","9492","9493","9494","9495","9496","9497",
    "9498","9499","9500","9501","9518","9519","9520","9521","9522","9523",
    "9524","9526","9527","9528","9529","9532","9533","9534","9535","9536",
    "9537","9539","9540","9541","9542","9543","9545","9546","9547","9548",
    "9549","9550","9551","9552","9553","9554","9555","9556","9557","9558",
    "9559","9560","9561","9562","9563","9564"
}  #  500 / 5% / 10
FILE_FILTER = (('CSV Files (*.csv)', '*.csv'),)



ZERO_EPS = 1e-6

def _parse_csv_list(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]

def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "").strip()
        return int(v) if v else default
    except Exception:
        return default

# ----- ENV (mirroring server.js constants) -----
# Treat empty env vars as "unset" so `.env` lines like `V2_BASE=` don't clobber defaults.
V2_BASE = (os.getenv("V2_BASE") or "https://practicecymliving.appfolio.com/api/v2/reports").rstrip("/")
V2_USER = os.getenv("V2_USER", "")
V2_PASS = os.getenv("V2_PASS", "")
V2_ORIGIN = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(V2_BASE))

V0_BASE = (os.getenv("V0_BASE") or "https://api.appfolio.com/api/v0").rstrip("/")
V0_DEV_ID = os.getenv("V0_DEV_ID", "")
V0_CLIENT_ID = os.getenv("V0_CLIENT_ID", "")
V0_CLIENT_SECRET = os.getenv("V0_CLIENT_SECRET", "")

BULK_GL_ACCOUNT_ID = os.getenv("BULK_GL_ACCOUNT_ID", "")
TABLE_GL_ACCOUNT_NUMBER = os.getenv("TABLE_GL_ACCOUNT_NUMBER", "4815-000")
FILTER_GL_ACCOUNT = os.getenv("FILTER_GL_ACCOUNT", "4021-000")

DEFAULT_V0_DAYS = 30
DEFAULT_V2_PROPERTY_IDS = ["8006","8007","8008","8009","8010","8011","8012","8013","8014","8015"]
V2_PROPERTY_IDS = _parse_csv_list(os.getenv("V2_PROPERTY_IDS", "").strip())
CACHE_TTL_SECONDS = max(0, _env_int("CACHE_TTL_SECONDS", 300))
HTTP_POOL_MAXSIZE = max(1, _env_int("HTTP_POOL_MAXSIZE", 10))
_v2_mode = (os.getenv("V2_TENANT_COLUMNS_MODE", "columns_first") or "columns_first").strip().lower()
V2_TENANT_COLUMNS_MODE = _v2_mode if _v2_mode in ("columns_first", "filters_only") else "columns_first"

# ----- Helpers ported from server.js -----
MONTH_ABBR = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

def resource_path(*parts):
    base = getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__file__)))
    return os.path.join(base, *parts)

HERE = os.path.abspath(os.path.dirname(__file__))
UI_DIR = resource_path("ui")   # <- instead of os.path.join(HERE, "ui")

def _cache_get(key: str, ttl: int, refresh: bool):
    if refresh or ttl <= 0:
        return None
    now = time.time()
    with _CACHE_LOCK:
        item = _CACHE.get(key)
        if not item:
            return None
        ts, val = item
        if now - ts > ttl:
            _CACHE.pop(key, None)
            return None
        return val

def _cache_set(key: str, val: Any):
    if CACHE_TTL_SECONDS <= 0:
        return
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), val)

def _cached_fetch(key: str, refresh: bool, fn):
    cached = _cache_get(key, CACHE_TTL_SECONDS, refresh)
    if cached is not None:
        return cached
    val = fn()
    _cache_set(key, val)
    return val

def _format_server_timing(timings: Dict[str, float]) -> str:
    parts = []
    for k, v in timings.items():
        parts.append(f"{k};dur={v*1000:.1f}")
    return ", ".join(parts)

def _make_session() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=HTTP_POOL_MAXSIZE, pool_maxsize=HTTP_POOL_MAXSIZE)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def first_of_month_mmddyyyy_from_ymd(ymd: str) -> str:
    """
    Given YYYY-MM-DD, return MM/01/YYYY. If bad input, use current month.
    """
    try:
        y = int(ymd[0:4])
        m = int(ymd[5:7])
    except Exception:
        now = datetime.utcnow()
        y, m = now.year, now.month
    return f"{m:02d}/01/{y:04d}"

def late_fee_params_for_prop(prop_id: Any) -> tuple[float, float, float]:
    """
    Returns (threshold, percent, base) for a given v2 property_id.
    Falls back to env-based defaults if prop_id isn't in a group.
    """
    pid = str(prop_id or "").strip()
    if pid in PROPERTY_GROUP_A:
        return 1000.0, 0.05, 10.0
    if pid in PROPERTY_GROUP_B:
        return 500.0, 0.05, 10.0
    # fallback to env-configured defaults (safety / future props)
    return LATE_FEE_THRESHOLD, LATE_FEE_PERCENT, LATE_FEE_BASE_FEE

def computeLateFee(total_amount: Any, zero_to_30: Any, *, threshold: float, percent: float, base: float) -> float:
    """
    Rules:
      - If 0_to30 == 0 → Amount = 0
      - If 0_to30 > 0 and total_amount > threshold → (total - threshold)*percent + base
      - If 0_to30 > 0 and total_amount <= threshold → base
    """
    total = parseCurrencyOrNumber(total_amount)
    z = parseCurrencyOrNumber(zero_to_30)
    if z <= ZERO_EPS:
        return 0.0
    return ((total - threshold) * percent + base) if total > threshold else base



def post_v0_charge(payload: dict) -> dict:
    url = f"{V0_BASE}/charges"
    resp = requests.post(
        url,
        headers={
            "X-AppFolio-Developer-ID": V0_DEV_ID,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": authV0(),
        },
        json=payload,
        timeout=60,
    )
    text = resp.text
    try:
        j = resp.json()
    except Exception:
        j = {"raw": text}
    if not resp.ok:
        # Bubble up exact API error body to help you debug
        raise HTTPException(status_code=resp.status_code, detail=f"Create charge failed: {text}")
    return j

# --- Add these imports at top if missing ---
from datetime import date  # you already have datetime, timedelta

def end_of_current_month_ymd() -> str:
    """YYYY-MM-DD for the last day of the current UTC month."""
    today = datetime.utcnow().date()
    if today.month == 12:
        first_next = date(today.year + 1, 1, 1)
    else:
        first_next = date(today.year, today.month + 1, 1)
    last = first_next - timedelta(days=1)
    return last.isoformat()

def v2_post_json(url: str, body: Optional[dict] = None, timeout: int = 60, session: Optional[requests.Session] = None) -> dict:
    """
    Always POST. If body is None, send an empty JSON object {} and include Content-Type.
    This avoids 406 responses some stacks return for POSTs with no body.
    Retries on 429/5xx.
    """
    payload = {} if body is None else body
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",     # always set
        "Authorization": authV2(),
    }
    backoff = 0.5
    s = session or requests
    for attempt in range(6):
        resp = s.post(url, headers=headers, json=payload, timeout=timeout)
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < 5:
            time.sleep(backoff); backoff *= 2; continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("Unreachable")

def v2_collect_all(path: str, first_body: dict) -> List[dict]:
    """
    POST the report once with filters/columns; then POST each next_page_url with {}.
    Return a flat list of rows.
    """
    session = _make_session()
    first_url = f"{V2_BASE.rstrip('/')}/{path.lstrip('/')}"
    js = v2_post_json(first_url, first_body, session=session)
    rows: List[dict] = []
    if isinstance(js, list):
        rows.extend(js)
        next_url = None
    else:
        rows.extend(js.get("results", []))
        next_url = js.get("next_page_url")

    def abs_url(u: str) -> str:
        return u if u.startswith("http") else urljoin(V2_ORIGIN, u)

    while next_url:
        js = v2_post_json(abs_url(next_url), None, session=session)   # POST {} (see v2_post_json)
        rows.extend(js.get("results", []))
        next_url = js.get("next_page_url")
    return rows


def toYMD(s: Any) -> str:
    if not s:
        return ""
    s = str(s)
    # YYYY-MM-DD
    import re
    m1 = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m1:
        return f"{m1.group(1)}-{m1.group(2)}-{m1.group(3)}"
    # MM/DD/YYYY
    m2 = re.match(r"^(\d{1,2})\/(\d{1,2})\/(\d{4})$", s)
    if m2:
        mm = m2.group(1).zfill(2)
        dd = m2.group(2).zfill(2)
        return f"{m2.group(3)}-{mm}-{dd}"
    try:
        d = datetime.fromisoformat(s.replace("Z","").replace("T"," "))
    except Exception:
        try:
            from dateutil import parser as dp  # optional if installed
            d = dp.parse(s)
        except Exception:
            return ""
    return d.strftime("%Y-%m-%d")

def toMMDDYYYY(dateish: Any) -> str:
    try:
        d = datetime.fromisoformat(str(dateish).replace("Z","").replace("T"," "))
    except Exception:
        try:
            from dateutil import parser as dp
            d = dp.parse(str(dateish))
        except Exception:
            return ""
    return f"{d.month:02d}/{d.day:02d}/{d.year}"

def isoDaysAgo(days: int) -> str:
    ms = datetime.utcnow() - timedelta(days=days)
    return ms.isoformat() + "Z"

def parseCurrencyOrNumber(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(str(v).replace(",","").replace("$","").strip())
    except Exception:
        return 0.0

def computeAmountFromZeroTo30(v: Any) -> float:
    H = parseCurrencyOrNumber(v)
    return (H - 500) * 0.05 + 10 if H > 500 else 10.0

def lastCommaFirstToFirstLast(name: str) -> str:
    if not name:
        return ""
    parts = [p.strip() for p in str(name).split(",")]
    if len(parts) >= 2:
        return f"{parts[1]} {parts[0]}".strip()
    return name or ""

def lateFeeDescriptionFromYMD(ymd: str) -> str:
    if not ymd:
        return "Late Rent Charges - Late fee"
    try:
        mm = int(str(ymd)[5:7])
    except Exception:
        return "Late Rent Charges - Late fee"
    abbr = MONTH_ABBR[mm-1] if 1 <= mm <= 12 else None
    return f"Late Rent Charges - Late fee for {abbr}" if abbr else "Late Rent Charges - Late fee"

def clampInt(n, minv, maxv) -> Optional[int]:
    try:
        n = int(float(n))
        return max(minv, min(maxv, n))
    except Exception:
        return None

# Auth headers
def authV2() -> str:
    raw = f"{V2_USER}:{V2_PASS}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")

def authV0() -> str:
    raw = f"{V0_CLIENT_ID}:{V0_CLIENT_SECRET}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")

# Fast fetch wrapper
def fetchFast(method: str, url: str, *, headers=None, json_body=None, timeout=20, session: Optional[requests.Session] = None) -> requests.Response:
    headers = headers or {}
    s = session or requests
    return s.request(method, url, headers=headers, json=json_body, timeout=timeout)

# ---------- V2 pagination like server.js ----------
def postV2ReportAll(path: str, body: Optional[dict] = None) -> List[dict]:
    """Back-compat shim: POST-first, then POST next_page_url's. Returns one dict with 'results'."""
    rows = v2_collect_all(path, body or {})
    return [{"results": rows}]

def postV2ReportAllSafe(path: str, body_with_columns: dict, fallback_body: dict | None = None) -> List[dict]:
    fallback_body = fallback_body or {}
    try:
        return postV2ReportAll(path, body_with_columns)
    except Exception as e:
        # Unconditionally attempt fallback; different tenants return different error texts.
        try:
            return postV2ReportAll(path, fallback_body)
        except Exception:
            # Re-raise the original error so /debug endpoints still surface the root cause.
            raise e


def normalizeV2ResultsPages(pages: List[dict]) -> List[dict]:
    out: List[dict] = []
    for p in pages or []:
        results = p.get("results")
        if isinstance(results, list):
            out.extend(results)
    return out

# ---------- V2 REPORT CALLS ----------
def fetchAgedReceivables() -> List[dict]:
    eom = end_of_current_month_ymd()
    columns = [
    "property_name","unit_name","payer_name","occupancy_id",
    "0_to30","total_amount","account_number",
    "unit_id","property_id","posting_date","invoice_occurred_on"
    ]
    first_body = {
        "occurred_on_to": eom,
        "property_visibility": "active",
        "tenant_statuses": ["0","4","3"],
        "properties": {
            "properties_ids": V2_PROPERTY_IDS
        },
        "columns": columns,
    }
    all_rows = v2_collect_all("aged_receivables_detail.json", first_body)

    # Local GL filter
    if str(FILTER_GL_ACCOUNT or "").strip():
        filtered = [r for r in all_rows if str(r.get("account_number","")).strip() == FILTER_GL_ACCOUNT]
    else:
        filtered = list(all_rows)

    out = []
    for r in filtered:
        out.append({
            "propName": r.get("property_name",""),
            "unitName": r.get("unit_name",""),
            "payerName": r.get("payer_name",""),
            "occIdV2": r.get("occupancy_id",""),
            "zeroTo30": r.get("0_to30",0),
            "totalAmount": r.get("total_amount", 0),
            "v2UnitId": r.get("unit_id",""),
            "v2PropId": r.get("property_id",""),
            "postingDateRaw": r.get("posting_date",""),
            "chargeDateRaw": r.get("invoice_occurred_on",""),
            "_account_number": r.get("account_number",""),
        })
    return out

def fetchTenantDirectory() -> List[dict]:
    """
    Match the working curl first: POST with filters only (no columns).
    If that ever fails, progressively try minimal column sets that are widely supported.
    """
    # 1) EXACTLY your working curl body
    body_filters_only = {
        "tenant_visibility": "active",
        "tenant_statuses": ["0", "4","3"],
        "tenant_types": ["all"],
        "property_visibility": "active",
        "properties": {
            "properties_ids": V2_PROPERTY_IDS
        }
    }

    # 2) Minimal columns (common across stacks)
    cols_min_unit = ["property_name", "unit", "occupancy_import_uid", "tenant_integration_id", "status"]
    cols_min_unit_name = ["property_name", "unit_name", "occupancy_import_uid", "tenant_integration_id", "status"]

    if V2_TENANT_COLUMNS_MODE == "filters_only":
        bodies: List[dict] = [
            body_filters_only,
            {**body_filters_only, "columns": cols_min_unit},
            {**body_filters_only, "columns": cols_min_unit_name},
            {"tenant_statuses": ["0", "4", "3"], "property_visibility": "active"},
            {"tenant_statuses": ["0", "4", "3"], "property_visibility": "active", "columns": cols_min_unit},
        ]
    else:
        bodies = [
            {**body_filters_only, "columns": cols_min_unit},
            {**body_filters_only, "columns": cols_min_unit_name},
            body_filters_only,
            {"tenant_statuses": ["0", "4", "3"], "property_visibility": "active"},
            {"tenant_statuses": ["0", "4", "3"], "property_visibility": "active", "columns": cols_min_unit},
        ]

    last_err: Optional[Exception] = None
    for b in bodies:
        try:
            return v2_collect_all("tenant_directory.json", b)
        except Exception as e:
            last_err = e
            continue
    raise requests.HTTPError(f"tenant_directory failed for all bodies: {last_err}")

# ---------- V0 paged fetch ----------

V0_PROPERTY_IDS = os.getenv("V0_PROPERTY_IDS", "").strip()

def _parse_id_list(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]

def fetchV0All(path: str, page_size: int = 1000, base_query: Dict[str,str] | None = None, session: Optional[requests.Session] = None) -> List[dict]:
    base_query = base_query or {}
    all_data: List[dict] = []
    page = 1
    s = session or _make_session()
    while True:
        q = base_query.copy()
        q["page[number]"] = str(page)
        q["page[size]"] = str(page_size)
        url = f"{V0_BASE}/{path}?{urlencode(q)}"
        # bump timeout a bit for big "all tenants" pulls
        resp = fetchFast("GET", url, headers={
            "X-AppFolio-Developer-ID": V0_DEV_ID,
            "Accept": "application/json",
            "Authorization": authV0(),
        }, timeout=45, session=s)
        txt = resp.text
        try:
            j = resp.json()
        except Exception:
            j = {"raw": txt}
        if not resp.ok:
            raise RuntimeError(f"{path} failed: {resp.status_code} {txt}")
        data = j.get("data", [])
        if isinstance(data, list):
            all_data.extend(data)
        if not data or len(data) < page_size:
            break
        page += 1
    return all_data

def fetchV0TenantsAll(days: Optional[int] = DEFAULT_V0_DAYS) -> List[dict]:
    # Always include LastUpdatedAtFrom; if days is None, use a wide window (10y)
    lookback = clampInt(days, 1, 3650) if days is not None else 3650
    base_query = {
        "filters[Status]": "Current,Notice,Evict",
        "filters[IncludeUnassigned]": "false",
        "filters[LastUpdatedAtFrom]": isoDaysAgo(lookback or 3650),
    }
    return fetchV0All("tenants", page_size=1000, base_query=base_query)

def fetchV0Tenants_31days_specific_props(days: int = DEFAULT_V0_DAYS) -> List[dict]:
    """
    GET /api/v0/tenants with:
      - LastUpdatedAtFrom = now - N days
      - PropertyId = comma-separated list (from ENV or fallback to your provided list)
      - Status=Current,Notice
      - IncludeUnassigned=false
    """
    prop_ids = _parse_id_list(V0_PROPERTY_IDS) or [
        "b557f684-7a33-11f0-af0a-02e94e52d34b","b57b5483-7a33-11f0-af0a-02e94e52d34b","b5a46f95-7a33-11f0-af0a-02e94e52d34b","b5cf3867-7a33-11f0-af0a-02e94e52d34b",
        "f54d48a2-bc25-11f0-8ab6-12de4bf481cd","f56cd263-bc25-11f0-8ab6-12de4bf481cd","f5a859e1-bc25-11f0-8ab6-12de4bf481cd","f5c0b4ec-bc25-11f0-8ab6-12de4bf481cd",
        "f5ebf440-bc25-11f0-8ab6-12de4bf481cd","f6062114-bc25-11f0-8ab6-12de4bf481cd","f62d2f82-bc25-11f0-8ab6-12de4bf481cd","f640d332-bc25-11f0-8ab6-12de4bf481cd",
        "f647f6ff-bc25-11f0-8ab6-12de4bf481cd","f675f10f-bc25-11f0-8ab6-12de4bf481cd","f6964cdd-bc25-11f0-8ab6-12de4bf481cd","f6ab8584-bc25-11f0-8ab6-12de4bf481cd",
        "f6b670cc-bc25-11f0-8ab6-12de4bf481cd","f6fa6db1-bc25-11f0-8ab6-12de4bf481cd","f6fe0be5-bc25-11f0-8ab6-12de4bf481cd",
        "f7003f55-bc25-11f0-8ab6-12de4bf481cd","fb9faee6-bc25-11f0-8ab6-12de4bf481cd","fbfd3faf-bc25-11f0-8ab6-12de4bf481cd","fd2bda12-bc25-11f0-8ab6-12de4bf481cd",
        "fd74c2f4-bc25-11f0-8ab6-12de4bf481cd","fdbae8e0-bc25-11f0-8ab6-12de4bf481cd","00ca034c-bc26-11f0-8ab6-12de4bf481cd","0271b6d4-bc26-11f0-8ab6-12de4bf481cd",
        "03ee97ec-bc26-11f0-8ab6-12de4bf481cd","0889eefe-bc26-11f0-8ab6-12de4bf481cd","08e4277d-bc26-11f0-8ab6-12de4bf481cd","09ee1804-bc26-11f0-8ab6-12de4bf481cd",
        "0a49570c-bc26-11f0-8ab6-12de4bf481cd","0a9e6864-bc26-11f0-8ab6-12de4bf481cd","0af5e04c-bc26-11f0-8ab6-12de4bf481cd","0b49ff5b-bc26-11f0-8ab6-12de4bf481cd",
        "0b9f16cc-bc26-11f0-8ab6-12de4bf481cd","0bf6273a-bc26-11f0-8ab6-12de4bf481cd","0c4d7239-bc26-11f0-8ab6-12de4bf481cd","0d004944-bc26-11f0-8ab6-12de4bf481cd",
        "0d545bb5-bc26-11f0-8ab6-12de4bf481cd","0da6f612-bc26-11f0-8ab6-12de4bf481cd","0e0624f7-bc26-11f0-8ab6-12de4bf481cd","0e5a4232-bc26-11f0-8ab6-12de4bf481cd",
        "0ea9e7ee-bc26-11f0-8ab6-12de4bf481cd","0efd3758-bc26-11f0-8ab6-12de4bf481cd","116f2cf6-bc26-11f0-8ab6-12de4bf481cd","11b4bbc8-bc26-11f0-8ab6-12de4bf481cd",
        "179cd2ef-bc26-11f0-8ab6-12de4bf481cd","203ef220-bc26-11f0-8ab6-12de4bf481cd","207a84bd-bc26-11f0-8ab6-12de4bf481cd","20b7f71a-bc26-11f0-8ab6-12de4bf481cd",
        "20f71cdb-bc26-11f0-8ab6-12de4bf481cd","2133e5bb-bc26-11f0-8ab6-12de4bf481cd","216aea35-bc26-11f0-8ab6-12de4bf481cd","21a94f49-bc26-11f0-8ab6-12de4bf481cd",
        "21e8ca31-bc26-11f0-8ab6-12de4bf481cd","222a4c85-bc26-11f0-8ab6-12de4bf481cd","226ae6aa-bc26-11f0-8ab6-12de4bf481cd","22af2157-bc26-11f0-8ab6-12de4bf481cd",
        "22f48e84-bc26-11f0-8ab6-12de4bf481cd","23359b18-bc26-11f0-8ab6-12de4bf481cd","23786630-bc26-11f0-8ab6-12de4bf481cd","23f0771c-bc26-11f0-8ab6-12de4bf481cd",
        "24e09c9e-bc26-11f0-8ab6-12de4bf481cd","26c80e04-bc26-11f0-8ab6-12de4bf481cd","27fc5fbf-bc26-11f0-8ab6-12de4bf481cd","283769a4-bc26-11f0-8ab6-12de4bf481cd",
        "29e3f96c-bc26-11f0-8ab6-12de4bf481cd","2b51a2d9-bc26-11f0-8ab6-12de4bf481cd","f74e071c-bc25-11f0-8ab6-12de4bf481cd",
        "f75c19ce-bc25-11f0-8ab6-12de4bf481cd","f78e0a8a-bc25-11f0-8ab6-12de4bf481cd","f7b1535d-bc25-11f0-8ab6-12de4bf481cd","f7ca4617-bc25-11f0-8ab6-12de4bf481cd",
        "f800e102-bc25-11f0-8ab6-12de4bf481cd","f807a704-bc25-11f0-8ab6-12de4bf481cd","f80b2e6d-bc25-11f0-8ab6-12de4bf481cd","f842618a-bc25-11f0-8ab6-12de4bf481cd",
        "f84a0b49-bc25-11f0-8ab6-12de4bf481cd","f85f5249-bc25-11f0-8ab6-12de4bf481cd","f8902e3a-bc25-11f0-8ab6-12de4bf481cd","f89272bb-bc25-11f0-8ab6-12de4bf481cd",
        "f8b2b2f1-bc25-11f0-8ab6-12de4bf481cd","f8cb6078-bc25-11f0-8ab6-12de4bf481cd","f8da6701-bc25-11f0-8ab6-12de4bf481cd","f8dacd6d-bc25-11f0-8ab6-12de4bf481cd",
        "f9066ecf-bc25-11f0-8ab6-12de4bf481cd","f9190c86-bc25-11f0-8ab6-12de4bf481cd","f91ba815-bc25-11f0-8ab6-12de4bf481cd","f9383a6f-bc25-11f0-8ab6-12de4bf481cd",
        "f9536c44-bc25-11f0-8ab6-12de4bf481cd","f959e367-bc25-11f0-8ab6-12de4bf481cd","f961a52f-bc25-11f0-8ab6-12de4bf481cd","f9849ee3-bc25-11f0-8ab6-12de4bf481cd",
        "f9ab8292-bc25-11f0-8ab6-12de4bf481cd","f9abaf94-bc25-11f0-8ab6-12de4bf481cd","f9adaf30-bc25-11f0-8ab6-12de4bf481cd","f9d53f3b-bc25-11f0-8ab6-12de4bf481cd",
        "f9ea6492-bc25-11f0-8ab6-12de4bf481cd","f9fb4480-bc25-11f0-8ab6-12de4bf481cd","f9fd771b-bc25-11f0-8ab6-12de4bf481cd","fa2111f2-bc25-11f0-8ab6-12de4bf481cd",
        "fa265c37-bc25-11f0-8ab6-12de4bf481cd","fa3feb10-bc25-11f0-8ab6-12de4bf481cd","fa4befbd-bc25-11f0-8ab6-12de4bf481cd","fa740244-bc25-11f0-8ab6-12de4bf481cd",
        "fa7b70a9-bc25-11f0-8ab6-12de4bf481cd","fa825628-bc25-11f0-8ab6-12de4bf481cd","fa9dc0c1-bc25-11f0-8ab6-12de4bf481cd","fabbf343-bc25-11f0-8ab6-12de4bf481cd",
        "fac60dd1-bc25-11f0-8ab6-12de4bf481cd","fb9d1075-bc25-11f0-8ab6-12de4bf481cd","fb9d0995-bc25-11f0-8ab6-12de4bf481cd","fba01042-bc25-11f0-8ab6-12de4bf481cd",
        "fbe1c6da-bc25-11f0-8ab6-12de4bf481cd","fbe20fdd-bc25-11f0-8ab6-12de4bf481cd","fbfdaef7-bc25-11f0-8ab6-12de4bf481cd","fc1e4089-bc25-11f0-8ab6-12de4bf481cd",
        "fc1e5be6-bc25-11f0-8ab6-12de4bf481cd","fc4b4ecf-bc25-11f0-8ab6-12de4bf481cd","fc52d429-bc25-11f0-8ab6-12de4bf481cd","fc586bfb-bc25-11f0-8ab6-12de4bf481cd",
        "fc5bfae8-bc25-11f0-8ab6-12de4bf481cd","fca3a8b8-bc25-11f0-8ab6-12de4bf481cd","fca3e0fb-bc25-11f0-8ab6-12de4bf481cd","fca65d9a-bc25-11f0-8ab6-12de4bf481cd",
        "fcb0434a-bc25-11f0-8ab6-12de4bf481cd","fcecb855-bc25-11f0-8ab6-12de4bf481cd","fcedc3dd-bc25-11f0-8ab6-12de4bf481cd","fcf81dee-bc25-11f0-8ab6-12de4bf481cd",
        "fd05db42-bc25-11f0-8ab6-12de4bf481cd","fd2c7b54-bc25-11f0-8ab6-12de4bf481cd","fd4abc76-bc25-11f0-8ab6-12de4bf481cd","fd5adc2a-bc25-11f0-8ab6-12de4bf481cd",
        "fd7423d1-bc25-11f0-8ab6-12de4bf481cd","fd9745fd-bc25-11f0-8ab6-12de4bf481cd","fdaaf2e2-bc25-11f0-8ab6-12de4bf481cd","fdbaa14f-bc25-11f0-8ab6-12de4bf481cd",
        "fde5d091-bc25-11f0-8ab6-12de4bf481cd","fdfc576b-bc25-11f0-8ab6-12de4bf481cd","fe06755b-bc25-11f0-8ab6-12de4bf481cd","fe094cb7-bc25-11f0-8ab6-12de4bf481cd",
        "fe3a4e3e-bc25-11f0-8ab6-12de4bf481cd","fe4bf6b0-bc25-11f0-8ab6-12de4bf481cd","fe4c7dbf-bc25-11f0-8ab6-12de4bf481cd","fe4e0f20-bc25-11f0-8ab6-12de4bf481cd",
        "fe87181a-bc25-11f0-8ab6-12de4bf481cd","fe9179c7-bc25-11f0-8ab6-12de4bf481cd","fe93b53c-bc25-11f0-8ab6-12de4bf481cd","fea2bbe6-bc25-11f0-8ab6-12de4bf481cd",
        "fed0a15a-bc25-11f0-8ab6-12de4bf481cd","fed9d6af-bc25-11f0-8ab6-12de4bf481cd","fedc0384-bc25-11f0-8ab6-12de4bf481cd","fef40c21-bc25-11f0-8ab6-12de4bf481cd",
        "ff0f12ce-bc25-11f0-8ab6-12de4bf481cd","ff1c1716-bc25-11f0-8ab6-12de4bf481cd","ff2c9ece-bc25-11f0-8ab6-12de4bf481cd","ff4a64ac-bc25-11f0-8ab6-12de4bf481cd",
        "ff4d2eed-bc25-11f0-8ab6-12de4bf481cd","ff5a7778-bc25-11f0-8ab6-12de4bf481cd","ff806bef-bc25-11f0-8ab6-12de4bf481cd","ff8ade2a-bc25-11f0-8ab6-12de4bf481cd",
        "ff9f56a9-bc25-11f0-8ab6-12de4bf481cd","ffa16ae2-bc25-11f0-8ab6-12de4bf481cd","ffd0219e-bc25-11f0-8ab6-12de4bf481cd","ffd1d1a2-bc25-11f0-8ab6-12de4bf481cd",
        "ffdfd34f-bc25-11f0-8ab6-12de4bf481cd","ffed53d2-bc25-11f0-8ab6-12de4bf481cd","001f161e-bc26-11f0-8ab6-12de4bf481cd","00213f6b-bc26-11f0-8ab6-12de4bf481cd",
        "003a31dd-bc26-11f0-8ab6-12de4bf481cd","003c6faf-bc26-11f0-8ab6-12de4bf481cd","005ddfdb-bc26-11f0-8ab6-12de4bf481cd","007721ac-bc26-11f0-8ab6-12de4bf481cd",
        "00915523-bc26-11f0-8ab6-12de4bf481cd","009e1d27-bc26-11f0-8ab6-12de4bf481cd","00e3b71f-bc26-11f0-8ab6-12de4bf481cd","00e5c232-bc26-11f0-8ab6-12de4bf481cd",
        "011c5d47-bc26-11f0-8ab6-12de4bf481cd","01257e31-bc26-11f0-8ab6-12de4bf481cd","013bd282-bc26-11f0-8ab6-12de4bf481cd","013df51e-bc26-11f0-8ab6-12de4bf481cd",
        "016a259c-bc26-11f0-8ab6-12de4bf481cd","017373d2-bc26-11f0-8ab6-12de4bf481cd","0185d849-bc26-11f0-8ab6-12de4bf481cd","01920ef7-bc26-11f0-8ab6-12de4bf481cd",
        "01c8773e-bc26-11f0-8ab6-12de4bf481cd","01ca7b5c-bc26-11f0-8ab6-12de4bf481cd","01f2eac6-bc26-11f0-8ab6-12de4bf481cd","01f84383-bc26-11f0-8ab6-12de4bf481cd",
        "020ac8a6-bc26-11f0-8ab6-12de4bf481cd","021fad26-bc26-11f0-8ab6-12de4bf481cd","024eb2e5-bc26-11f0-8ab6-12de4bf481cd","0250bfb5-bc26-11f0-8ab6-12de4bf481cd",
        "0270ffb6-bc26-11f0-8ab6-12de4bf481cd","0290c386-bc26-11f0-8ab6-12de4bf481cd","02a20015-bc26-11f0-8ab6-12de4bf481cd","02c870e6-bc26-11f0-8ab6-12de4bf481cd",
        "02e60218-bc26-11f0-8ab6-12de4bf481cd","02f1722d-bc26-11f0-8ab6-12de4bf481cd","02fb0cc5-bc26-11f0-8ab6-12de4bf481cd","03159be0-bc26-11f0-8ab6-12de4bf481cd",
        "03234770-bc26-11f0-8ab6-12de4bf481cd","0345c99f-bc26-11f0-8ab6-12de4bf481cd","03656e30-bc26-11f0-8ab6-12de4bf481cd","0365b78d-bc26-11f0-8ab6-12de4bf481cd",
        "036717f0-bc26-11f0-8ab6-12de4bf481cd","0396f3d1-bc26-11f0-8ab6-12de4bf481cd","03aca90d-bc26-11f0-8ab6-12de4bf481cd","03acef9a-bc26-11f0-8ab6-12de4bf481cd",
        "03bc5a47-bc26-11f0-8ab6-12de4bf481cd","03edfb84-bc26-11f0-8ab6-12de4bf481cd","03ef7cae-bc26-11f0-8ab6-12de4bf481cd","040cb596-bc26-11f0-8ab6-12de4bf481cd",
        "0435664c-bc26-11f0-8ab6-12de4bf481cd","043a94d0-bc26-11f0-8ab6-12de4bf481cd","043cbd9c-bc26-11f0-8ab6-12de4bf481cd","04629012-bc26-11f0-8ab6-12de4bf481cd",
        "04775104-bc26-11f0-8ab6-12de4bf481cd","04801aaa-bc26-11f0-8ab6-12de4bf481cd","049ae217-bc26-11f0-8ab6-12de4bf481cd","04b96fd2-bc26-11f0-8ab6-12de4bf481cd",
        "04be6878-bc26-11f0-8ab6-12de4bf481cd","04c0895d-bc26-11f0-8ab6-12de4bf481cd","04ecf11c-bc26-11f0-8ab6-12de4bf481cd","04fc2c94-bc26-11f0-8ab6-12de4bf481cd",
        "0501cc40-bc26-11f0-8ab6-12de4bf481cd","0514db8a-bc26-11f0-8ab6-12de4bf481cd","053ff40b-bc26-11f0-8ab6-12de4bf481cd","05403a90-bc26-11f0-8ab6-12de4bf481cd",
        "0579b92d-bc26-11f0-8ab6-12de4bf481cd","0596bd85-bc26-11f0-8ab6-12de4bf481cd","0597a86d-bc26-11f0-8ab6-12de4bf481cd","059eed8d-bc26-11f0-8ab6-12de4bf481cd",
        "05cb28be-bc26-11f0-8ab6-12de4bf481cd","05d6f979-bc26-11f0-8ab6-12de4bf481cd","05d83738-bc26-11f0-8ab6-12de4bf481cd","06332465-bc26-11f0-8ab6-12de4bf481cd",
        "063439d6-bc26-11f0-8ab6-12de4bf481cd","06357ce6-bc26-11f0-8ab6-12de4bf481cd","063c44df-bc26-11f0-8ab6-12de4bf481cd","068284c4-bc26-11f0-8ab6-12de4bf481cd",
        "0682c89a-bc26-11f0-8ab6-12de4bf481cd","068822bc-bc26-11f0-8ab6-12de4bf481cd","06c70fa8-bc26-11f0-8ab6-12de4bf481cd","06cf0cdc-bc26-11f0-8ab6-12de4bf481cd",
        "06d57232-bc26-11f0-8ab6-12de4bf481cd","06d68ab3-bc26-11f0-8ab6-12de4bf481cd","0709dddb-bc26-11f0-8ab6-12de4bf481cd","070a5a8f-bc26-11f0-8ab6-12de4bf481cd",
        "072da7e1-bc26-11f0-8ab6-12de4bf481cd","072e1cb9-bc26-11f0-8ab6-12de4bf481cd","074eefae-bc26-11f0-8ab6-12de4bf481cd","074ef2a6-bc26-11f0-8ab6-12de4bf481cd",
        "077bf825-bc26-11f0-8ab6-12de4bf481cd","0781a2ee-bc26-11f0-8ab6-12de4bf481cd","078d4bda-bc26-11f0-8ab6-12de4bf481cd","078d976f-bc26-11f0-8ab6-12de4bf481cd",
        "07cd47fd-bc26-11f0-8ab6-12de4bf481cd","07d467c6-bc26-11f0-8ab6-12de4bf481cd","07d6d88d-bc26-11f0-8ab6-12de4bf481cd","07dc8e01-bc26-11f0-8ab6-12de4bf481cd",
        "0809f866-bc26-11f0-8ab6-12de4bf481cd","081cf684-bc26-11f0-8ab6-12de4bf481cd","082e60d9-bc26-11f0-8ab6-12de4bf481cd","082eb505-bc26-11f0-8ab6-12de4bf481cd",
        "084ab7eb-bc26-11f0-8ab6-12de4bf481cd","0865fd07-bc26-11f0-8ab6-12de4bf481cd","0888d594-bc26-11f0-8ab6-12de4bf481cd","088dbc82-bc26-11f0-8ab6-12de4bf481cd",
        "08a7e2f7-bc26-11f0-8ab6-12de4bf481cd","08cdf7da-bc26-11f0-8ab6-12de4bf481cd","08e32ad4-bc26-11f0-8ab6-12de4bf481cd","08e412d3-bc26-11f0-8ab6-12de4bf481cd",
        "090f19c5-bc26-11f0-8ab6-12de4bf481cd","092d9f73-bc26-11f0-8ab6-12de4bf481cd","093ba139-bc26-11f0-8ab6-12de4bf481cd","093cb862-bc26-11f0-8ab6-12de4bf481cd",
        "0951ee26-bc26-11f0-8ab6-12de4bf481cd","096b9d7b-bc26-11f0-8ab6-12de4bf481cd","09963031-bc26-11f0-8ab6-12de4bf481cd","099839b9-bc26-11f0-8ab6-12de4bf481cd",
        "0998fedd-bc26-11f0-8ab6-12de4bf481cd","09a63ebe-bc26-11f0-8ab6-12de4bf481cd","09e0be0f-bc26-11f0-8ab6-12de4bf481cd","09ec1206-bc26-11f0-8ab6-12de4bf481cd",
        "09ee7800-bc26-11f0-8ab6-12de4bf481cd","0a1cf79d-bc26-11f0-8ab6-12de4bf481cd","0a319f23-bc26-11f0-8ab6-12de4bf481cd","0a49b56d-bc26-11f0-8ab6-12de4bf481cd",
        "0a5c781e-bc26-11f0-8ab6-12de4bf481cd","0a6d8fa5-bc26-11f0-8ab6-12de4bf481cd","0a96eeae-bc26-11f0-8ab6-12de4bf481cd","0a9bfc44-bc26-11f0-8ab6-12de4bf481cd",
        "0aa9ac07-bc26-11f0-8ab6-12de4bf481cd","0adf35e5-bc26-11f0-8ab6-12de4bf481cd","0af395a5-bc26-11f0-8ab6-12de4bf481cd","0af5fc8b-bc26-11f0-8ab6-12de4bf481cd",
        "0b1ebdd5-bc26-11f0-8ab6-12de4bf481cd","0b35529c-bc26-11f0-8ab6-12de4bf481cd","0b4a1fab-bc26-11f0-8ab6-12de4bf481cd","0b565bfb-bc26-11f0-8ab6-12de4bf481cd",
        "0b74b8dc-bc26-11f0-8ab6-12de4bf481cd","0b97b20b-bc26-11f0-8ab6-12de4bf481cd","0b9c2281-bc26-11f0-8ab6-12de4bf481cd","0bb71dc2-bc26-11f0-8ab6-12de4bf481cd",
        "0bdb6027-bc26-11f0-8ab6-12de4bf481cd","0bf0294e-bc26-11f0-8ab6-12de4bf481cd","0bf49b6a-bc26-11f0-8ab6-12de4bf481cd","0c1548bc-bc26-11f0-8ab6-12de4bf481cd",
        "0c3a5995-bc26-11f0-8ab6-12de4bf481cd","0c4db2d4-bc26-11f0-8ab6-12de4bf481cd","0c5a977a-bc26-11f0-8ab6-12de4bf481cd","0c7e45af-bc26-11f0-8ab6-12de4bf481cd",
        "0c9fdfc1-bc26-11f0-8ab6-12de4bf481cd","0ca1dab5-bc26-11f0-8ab6-12de4bf481cd","0cad703a-bc26-11f0-8ab6-12de4bf481cd","0cb7cf3b-bc26-11f0-8ab6-12de4bf481cd",
        "0ceabd93-bc26-11f0-8ab6-12de4bf481cd","0cec9b3c-bc26-11f0-8ab6-12de4bf481cd","0cf3be7d-bc26-11f0-8ab6-12de4bf481cd","0d2db151-bc26-11f0-8ab6-12de4bf481cd",
        "0d3192c6-bc26-11f0-8ab6-12de4bf481cd","0d44504a-bc26-11f0-8ab6-12de4bf481cd","0d70a293-bc26-11f0-8ab6-12de4bf481cd","0d781472-bc26-11f0-8ab6-12de4bf481cd",
        "0d97a265-bc26-11f0-8ab6-12de4bf481cd","0db2b7c0-bc26-11f0-8ab6-12de4bf481cd","0db82878-bc26-11f0-8ab6-12de4bf481cd","0de8c5c7-bc26-11f0-8ab6-12de4bf481cd",
        "0e04cafa-bc26-11f0-8ab6-12de4bf481cd","0e04bb1e-bc26-11f0-8ab6-12de4bf481cd","0e420f61-bc26-11f0-8ab6-12de4bf481cd","0e464aef-bc26-11f0-8ab6-12de4bf481cd",
        "0e47ad66-bc26-11f0-8ab6-12de4bf481cd","0e87d4d3-bc26-11f0-8ab6-12de4bf481cd","0e88080a-bc26-11f0-8ab6-12de4bf481cd","0e9816cd-bc26-11f0-8ab6-12de4bf481cd",
        "0ecad75e-bc26-11f0-8ab6-12de4bf481cd","0ecb19c6-bc26-11f0-8ab6-12de4bf481cd","0eecaf80-bc26-11f0-8ab6-12de4bf481cd","0f0c7276-bc26-11f0-8ab6-12de4bf481cd",
        "0f0cb0d6-bc26-11f0-8ab6-12de4bf481cd","0f3d3ab5-bc26-11f0-8ab6-12de4bf481cd","0f4a40c6-bc26-11f0-8ab6-12de4bf481cd","0f4e6fac-bc26-11f0-8ab6-12de4bf481cd",
        "0f50c6a8-bc26-11f0-8ab6-12de4bf481cd","0f917260-bc26-11f0-8ab6-12de4bf481cd","0f9486e8-bc26-11f0-8ab6-12de4bf481cd","0f973fe5-bc26-11f0-8ab6-12de4bf481cd",
        "0fa58999-bc26-11f0-8ab6-12de4bf481cd","0fd641f4-bc26-11f0-8ab6-12de4bf481cd","0fe41c85-bc26-11f0-8ab6-12de4bf481cd","0fe51b6e-bc26-11f0-8ab6-12de4bf481cd",
        "0ff3748e-bc26-11f0-8ab6-12de4bf481cd","10196f60-bc26-11f0-8ab6-12de4bf481cd","1024e1cf-bc26-11f0-8ab6-12de4bf481cd","103877c9-bc26-11f0-8ab6-12de4bf481cd",
        "10435612-bc26-11f0-8ab6-12de4bf481cd","105e9125-bc26-11f0-8ab6-12de4bf481cd","1067f44e-bc26-11f0-8ab6-12de4bf481cd","10936d66-bc26-11f0-8ab6-12de4bf481cd",
        "109f7eef-bc26-11f0-8ab6-12de4bf481cd","10a22869-bc26-11f0-8ab6-12de4bf481cd","10a354e8-bc26-11f0-8ab6-12de4bf481cd","10e889a6-bc26-11f0-8ab6-12de4bf481cd",
        "10e9419b-bc26-11f0-8ab6-12de4bf481cd","10eaff15-bc26-11f0-8ab6-12de4bf481cd","10f8616d-bc26-11f0-8ab6-12de4bf481cd","112e5c9c-bc26-11f0-8ab6-12de4bf481cd",
        "11392fdf-bc26-11f0-8ab6-12de4bf481cd","113b8690-bc26-11f0-8ab6-12de4bf481cd","1145ca61-bc26-11f0-8ab6-12de4bf481cd","11986ecf-bc26-11f0-8ab6-12de4bf481cd",
        "1198e99b-bc26-11f0-8ab6-12de4bf481cd","11ec2628-bc26-11f0-8ab6-12de4bf481cd","11ecb5f2-bc26-11f0-8ab6-12de4bf481cd","11f4e8f4-bc26-11f0-8ab6-12de4bf481cd",
        "12340b77-bc26-11f0-8ab6-12de4bf481cd","124931a4-bc26-11f0-8ab6-12de4bf481cd","124a7011-bc26-11f0-8ab6-12de4bf481cd","129f4f90-bc26-11f0-8ab6-12de4bf481cd",
        "129fb0b7-bc26-11f0-8ab6-12de4bf481cd","12eeea04-bc26-11f0-8ab6-12de4bf481cd","12ef1efb-bc26-11f0-8ab6-12de4bf481cd","135437ad-bc26-11f0-8ab6-12de4bf481cd",
        "135499de-bc26-11f0-8ab6-12de4bf481cd","13b0e7db-bc26-11f0-8ab6-12de4bf481cd","13b16f1f-bc26-11f0-8ab6-12de4bf481cd","140c4aa2-bc26-11f0-8ab6-12de4bf481cd",
        "140c8557-bc26-11f0-8ab6-12de4bf481cd","146305af-bc26-11f0-8ab6-12de4bf481cd","146b3aaa-bc26-11f0-8ab6-12de4bf481cd","14c59ecd-bc26-11f0-8ab6-12de4bf481cd",
        "14c5c79f-bc26-11f0-8ab6-12de4bf481cd","15152ea9-bc26-11f0-8ab6-12de4bf481cd","151c3b4e-bc26-11f0-8ab6-12de4bf481cd","156c5cad-bc26-11f0-8ab6-12de4bf481cd",
        "1571b80b-bc26-11f0-8ab6-12de4bf481cd","15c487f0-bc26-11f0-8ab6-12de4bf481cd","15ca7ce5-bc26-11f0-8ab6-12de4bf481cd","1623bebc-bc26-11f0-8ab6-12de4bf481cd",
        "1623e95b-bc26-11f0-8ab6-12de4bf481cd","167f1358-bc26-11f0-8ab6-12de4bf481cd","167f308d-bc26-11f0-8ab6-12de4bf481cd","16d1cab3-bc26-11f0-8ab6-12de4bf481cd",
        "16db5d82-bc26-11f0-8ab6-12de4bf481cd","17358dbd-bc26-11f0-8ab6-12de4bf481cd","1735a0f9-bc26-11f0-8ab6-12de4bf481cd","17a37e22-bc26-11f0-8ab6-12de4bf481cd",
        "180e5c47-bc26-11f0-8ab6-12de4bf481cd","183dd421-bc26-11f0-8ab6-12de4bf481cd","1861991d-bc26-11f0-8ab6-12de4bf481cd","18b85f99-bc26-11f0-8ab6-12de4bf481cd",
        "18de107a-bc26-11f0-8ab6-12de4bf481cd","190e499d-bc26-11f0-8ab6-12de4bf481cd","19680272-bc26-11f0-8ab6-12de4bf481cd","1991a779-bc26-11f0-8ab6-12de4bf481cd",
        "19e259f0-bc26-11f0-8ab6-12de4bf481cd","1a405d01-bc26-11f0-8ab6-12de4bf481cd","1a5179a0-bc26-11f0-8ab6-12de4bf481cd","1a951fc4-bc26-11f0-8ab6-12de4bf481cd",
        "1abe9b64-bc26-11f0-8ab6-12de4bf481cd","1aebb593-bc26-11f0-8ab6-12de4bf481cd","1b4b54e2-bc26-11f0-8ab6-12de4bf481cd","1b515343-bc26-11f0-8ab6-12de4bf481cd",
        "1baa28f9-bc26-11f0-8ab6-12de4bf481cd","1bfb9da9-bc26-11f0-8ab6-12de4bf481cd","1c1bd61c-bc26-11f0-8ab6-12de4bf481cd","1c5b0cf2-bc26-11f0-8ab6-12de4bf481cd",
        "1c9841ae-bc26-11f0-8ab6-12de4bf481cd","1cd9d009-bc26-11f0-8ab6-12de4bf481cd","1d18606d-bc26-11f0-8ab6-12de4bf481cd","1d5678d2-bc26-11f0-8ab6-12de4bf481cd",
        "1d95d7da-bc26-11f0-8ab6-12de4bf481cd","1dd6f1db-bc26-11f0-8ab6-12de4bf481cd","1e1587e0-bc26-11f0-8ab6-12de4bf481cd","1e5283bc-bc26-11f0-8ab6-12de4bf481cd",
        "1e94c4d2-bc26-11f0-8ab6-12de4bf481cd","1ee3647a-bc26-11f0-8ab6-12de4bf481cd","1f506e23-bc26-11f0-8ab6-12de4bf481cd","1fb09d8f-bc26-11f0-8ab6-12de4bf481cd",
        "1ffbed28-bc26-11f0-8ab6-12de4bf481cd","251f37e1-bc26-11f0-8ab6-12de4bf481cd","255edcdd-bc26-11f0-8ab6-12de4bf481cd","25984bb7-bc26-11f0-8ab6-12de4bf481cd",
        "25d5d75a-bc26-11f0-8ab6-12de4bf481cd","26146d98-bc26-11f0-8ab6-12de4bf481cd","264e9da7-bc26-11f0-8ab6-12de4bf481cd","268e9eb1-bc26-11f0-8ab6-12de4bf481cd",
        "27032c4d-bc26-11f0-8ab6-12de4bf481cd","2746b652-bc26-11f0-8ab6-12de4bf481cd","2781d14d-bc26-11f0-8ab6-12de4bf481cd","27c16008-bc26-11f0-8ab6-12de4bf481cd",
        "2872680c-bc26-11f0-8ab6-12de4bf481cd","28a9dbbd-bc26-11f0-8ab6-12de4bf481cd","28e802f9-bc26-11f0-8ab6-12de4bf481cd","29248554-bc26-11f0-8ab6-12de4bf481cd",
        "29681512-bc26-11f0-8ab6-12de4bf481cd","29a619f1-bc26-11f0-8ab6-12de4bf481cd","2a228fce-bc26-11f0-8ab6-12de4bf481cd","2a5c1b65-bc26-11f0-8ab6-12de4bf481cd",
        "2a987469-bc26-11f0-8ab6-12de4bf481cd","2ad8aca7-bc26-11f0-8ab6-12de4bf481cd","2b15e7db-bc26-11f0-8ab6-12de4bf481cd","2b8e1b43-bc26-11f0-8ab6-12de4bf481cd",
        "2bc9dcd0-bc26-11f0-8ab6-12de4bf481cd","2c09b58e-bc26-11f0-8ab6-12de4bf481cd","2c4cd516-bc26-11f0-8ab6-12de4bf481cd","2c88d20f-bc26-11f0-8ab6-12de4bf481cd",
        "2cc530dd-bc26-11f0-8ab6-12de4bf481cd","2cfca6af-bc26-11f0-8ab6-12de4bf481cd","2d38c614-bc26-11f0-8ab6-12de4bf481cd","2d7c7ad8-bc26-11f0-8ab6-12de4bf481cd",
        "2dbe8d91-bc26-11f0-8ab6-12de4bf481cd","2dfda272-bc26-11f0-8ab6-12de4bf481cd","2e3aeb50-bc26-11f0-8ab6-12de4bf481cd","2e735eb4-bc26-11f0-8ab6-12de4bf481cd",
        "2eb1fdfc-bc26-11f0-8ab6-12de4bf481cd","2eed5046-bc26-11f0-8ab6-12de4bf481cd","2f2bc604-bc26-11f0-8ab6-12de4bf481cd","2f6bdaa1-bc26-11f0-8ab6-12de4bf481cd",
        "2fa6bd69-bc26-11f0-8ab6-12de4bf481cd","2fe7245c-bc26-11f0-8ab6-12de4bf481cd","30231fe0-bc26-11f0-8ab6-12de4bf481cd","b44a9b84-7a33-11f0-af0a-02e94e52d34b",
        "b471e129-7a33-11f0-af0a-02e94e52d34b","b49860ee-7a33-11f0-af0a-02e94e52d34b","b4d1d6b0-7a33-11f0-af0a-02e94e52d34b","b4fe4cee-7a33-11f0-af0a-02e94e52d34b",
        "b52b4753-7a33-11f0-af0a-02e94e52d34b"
    ]
    lookback = clampInt(days, 1, 3650) or DEFAULT_V0_DAYS
    q = {
        "filters[LastUpdatedAtFrom]": isoDaysAgo(lookback),
        "filters[PropertyId]": ",".join(prop_ids),
        "filters[Status]": "Current,Notice",
        "filters[IncludeUnassigned]": "false",
    }
    return fetchV0All("tenants", page_size=1000, base_query=q)

def _chunked(values: List[str], size: int) -> List[List[str]]:
    return [values[i:i + size] for i in range(0, len(values), size)]

def _clean_ids(values: List[Any]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        s = str(v or "").strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _build_v0_maps(v0tenants: List[dict]) -> tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    tenantId_to_v0OccId: Dict[str, str] = {}
    integrationId_to_v0OccId: Dict[str, str] = {}
    unitId_to_v0OccId: Dict[str, str] = {}
    for t in v0tenants:
        tid = str(t.get("Id", "")).strip()
        integ_id = str(t.get("IntegrationId", "") or t.get("ExternalId", "")).strip()
        occ = str(t.get("OccupancyId", "")).strip()
        status = str(t.get("Status", "")).lower()
        unitId = str(t.get("UnitId", "")).strip()
        if tid and occ:
            tenantId_to_v0OccId[tid] = occ
        if integ_id and occ:
            integrationId_to_v0OccId[integ_id] = occ
        if unitId and occ and status in ("current", "notice") and unitId not in unitId_to_v0OccId:
            unitId_to_v0OccId[unitId] = occ
    return tenantId_to_v0OccId, integrationId_to_v0OccId, unitId_to_v0OccId

def _apply_v0_maps(rows: List[Dict[str, Any]], tenantId_to_v0OccId: Dict[str, str], integrationId_to_v0OccId: Dict[str, str], unitId_to_v0OccId: Dict[str, str]):
    for rr in rows:
        if rr.get("_v0OccupancyId"):
            continue
        integ = rr.get("_tenantIntegrationId", "").strip()
        rr["_v0OccupancyId"] = (
            tenantId_to_v0OccId.get(integ, "") or
            integrationId_to_v0OccId.get(integ, "") or
            unitId_to_v0OccId.get(str(rr.get("_v2UnitId", "")), "")
        )

def fetchV0TenantsByIds(integration_ids: List[str], unit_ids: List[str]) -> List[dict]:
    integration_ids = _clean_ids(integration_ids)
    unit_ids = _clean_ids(unit_ids)
    if not integration_ids and not unit_ids:
        return []
    s = _make_session()
    seen: Dict[str, dict] = {}
    base = {
        "filters[Status]": "Current,Notice,Evict",
        "filters[IncludeUnassigned]": "false",
    }
    for chunk in _chunked(integration_ids, 100):
        q = dict(base)
        q["filters[IntegrationId]"] = ",".join(chunk)
        rows = fetchV0All("tenants", page_size=1000, base_query=q, session=s)
        for t in rows:
            tid = str(t.get("Id", "")).strip()
            if tid:
                seen[tid] = t
    for chunk in _chunked(unit_ids, 100):
        q = dict(base)
        q["filters[UnitId]"] = ",".join(chunk)
        rows = fetchV0All("tenants", page_size=1000, base_query=q, session=s)
        for t in rows:
            tid = str(t.get("Id", "")).strip()
            if tid:
                seen[tid] = t
    return list(seen.values())

def fetchAgedReceivables_cached(refresh: bool = False) -> List[dict]:
    props_key = ",".join(sorted(V2_PROPERTY_IDS))
    key = f"aged|{props_key}|{FILTER_GL_ACCOUNT}|{end_of_current_month_ymd()}"
    return _cached_fetch(key, refresh, fetchAgedReceivables)

def fetchTenantDirectory_cached(refresh: bool = False) -> List[dict]:
    props_key = ",".join(sorted(V2_PROPERTY_IDS))
    key = f"tdir|{props_key}|{V2_TENANT_COLUMNS_MODE}"
    return _cached_fetch(key, refresh, fetchTenantDirectory)

def fetchV0Tenants_cached(days: int, refresh: bool = False) -> List[dict]:
    prop_key = V0_PROPERTY_IDS.strip() or "fallback"
    key = f"v0tenants|{prop_key}|{days}"
    return _cached_fetch(key, refresh, lambda: fetchV0Tenants_31days_specific_props(days))

# ---------- FastAPI app (endpoints mirroring server.js) ----------
app = FastAPI(title="Bulk Charge Local API (Node Parity)")
app.add_middleware(GZipMiddleware, minimum_size=500)
api = APIRouter()

@api.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}

@api.get("/debug/aged")
def debug_aged():
    rows = fetchAgedReceivables()
    return {"count": len(rows), "sample": rows[0] if rows else None}

@api.get("/debug/aged-raw")
def debug_aged_raw():
    eom = end_of_current_month_ymd()
    body = {
        "occurred_on_to": eom,
        "property_visibility": "active",
        "tenant_statuses": ["0", "4", "3"],
        "properties": {"properties_ids": V2_PROPERTY_IDS},
        "columns": [
            "property_name",
            "unit_name",
            "payer_name",
            "occupancy_id",
            "0_to30",
            "total_amount",
            "account_number",
            "unit_id",
            "property_id",
            "posting_date",
            "invoice_occurred_on",
        ],
    }
    all_rows = v2_collect_all("aged_receivables_detail.json", body)
    acct_values = []
    seen = set()
    for r in all_rows:
        v = str(r.get("account_number", "")).strip()
        if v and v not in seen:
            seen.add(v)
            acct_values.append(v)
        if len(acct_values) >= 30:
            break
    if str(FILTER_GL_ACCOUNT or "").strip():
        filtered = [r for r in all_rows if str(r.get("account_number", "")).strip() == FILTER_GL_ACCOUNT]
    else:
        filtered = list(all_rows)
    return {
        "count_all": len(all_rows),
        "count_filtered": len(filtered),
        "filter_gl_account": FILTER_GL_ACCOUNT,
        "account_numbers_sample": acct_values,
        "properties_ids": V2_PROPERTY_IDS,
        "sample_all": all_rows[0] if all_rows else None,
    }

@api.get("/debug/tenant")
def debug_tenant():
    rows = fetchTenantDirectory()
    return {"count": len(rows), "sample": rows[0] if rows else None}


@api.get("/table-data")
def table_data(v0days: int = DEFAULT_V0_DAYS, refresh: int = 0, resolve_missing: int = 0, response: Response = None):
    total_start = time.perf_counter()
    timings: Dict[str, float] = {}
    try:
        d = clampInt(v0days, 1, 3650) or DEFAULT_V0_DAYS
        refresh_flag = bool(int(refresh))
        resolve_missing_flag = bool(int(resolve_missing))
        warnings: List[str] = []

        def submit_timed(executor, label: str, fn):
            def wrapped():
                t0 = time.perf_counter()
                try:
                    return fn()
                finally:
                    timings[label] = time.perf_counter() - t0
            return executor.submit(wrapped)

        with ThreadPoolExecutor(max_workers=3) as ex:
            fut_aged = submit_timed(ex, "fetch_aged", lambda: fetchAgedReceivables_cached(refresh_flag))
            fut_tdir = submit_timed(ex, "fetch_tdir", lambda: fetchTenantDirectory_cached(refresh_flag))
            fut_v0 = submit_timed(ex, "fetch_v0", lambda: fetchV0Tenants_cached(d, refresh_flag))

            try:
                aged = fut_aged.result()
            except Exception as e:
                print("aged_receivables failed:", repr(e))
                warnings.append(f"v2 aged receivables failed: {type(e).__name__}: {e}")
                aged = []

            try:
                tdir = fut_tdir.result()
            except Exception as e:
                print("tenant_directory failed:", repr(e))
                warnings.append(f"v2 tenant directory failed: {type(e).__name__}: {e}")
                tdir = []

            try:
                v0tenants = fut_v0.result()
            except Exception as e:
                print("v0 tenants failed:", repr(e))
                warnings.append(f"v0 tenants failed: {type(e).__name__}: {e}")
                v0tenants = []

        if not aged:
            if not warnings:
                warnings.append(
                    "No rows returned from V2 aged receivables (or all rows were filtered out). "
                    "Check /api/debug/aged-raw and verify V2 credentials, V2_PROPERTY_IDS, and FILTER_GL_ACCOUNT."
                )
            timings["total"] = time.perf_counter() - total_start
            if response is not None:
                response.headers["Server-Timing"] = _format_server_timing(timings)
            print("table_data timings:", _format_server_timing(timings))
            return {"rows": [], "warnings": warnings}

        t0_maps = time.perf_counter()
        tenantId_to_v0OccId, integrationId_to_v0OccId, unitId_to_v0OccId = _build_v0_maps(v0tenants)
        timings["v0_maps"] = time.perf_counter() - t0_maps

        def keyFrom(p: str, u: str) -> str:
            return f"{(p or '').strip().lower()}||{(u or '').strip().lower()}"

        occUid_to_candidates: Dict[str, List[Dict[str,str]]] = {}
        occUidByPropUnit: Dict[str, str] = {}
        occId_to_occUid: Dict[str, str] = {}

        for r in tdir:
            occ_uid = str(r.get("occupancy_import_uid","")).strip()
            integ   = str(r.get("tenant_integration_id","")).strip()
            status  = str(r.get("status","")).strip().lower()
            if occ_uid and integ:
                occUid_to_candidates.setdefault(occ_uid, []).append({"integ": integ, "status": status})
            prop = r.get("property_name") or r.get("property") or ""
            unit = r.get("unit") or r.get("unit_name") or ""
            if occ_uid and (prop or unit):
                occUidByPropUnit[keyFrom(prop, unit)] = occ_uid

            occ_id = str(r.get("occupancy_id","")).strip()
            if occ_id and occ_uid and occ_id not in occId_to_occUid:
                occId_to_occUid[occ_id] = occ_uid

        def pickIntegrationId(lst: List[Dict[str, str]]) -> str:
            if not lst:
                return ""
            for x in lst:
                if x.get("status") in ("current","notice"):
                    return x.get("integ","").strip()
            return lst[0].get("integ","").strip()

        t0_rows = time.perf_counter()
        rows: List[Dict[str, Any]] = []
        for r in aged:
            occ_v2 = str(r.get("occIdV2","")).strip()
            pk = keyFrom(r.get("propName",""), r.get("unitName",""))

            if occUid_to_candidates.get(occ_v2):
                occ_uid = occ_v2
            else:
                occ_uid = occUidByPropUnit.get(pk) or occId_to_occUid.get(occ_v2, "")

            cands = occUid_to_candidates.get(occ_uid, [])
            integ = pickIntegrationId(cands)

            v0OccId = (
                tenantId_to_v0OccId.get(integ, "") or
                integrationId_to_v0OccId.get(integ, "") or
                (unitId_to_v0OccId.get(str(r.get("v2UnitId","")), "") if r.get("v2UnitId") else "")
            )

            z_clean = parseCurrencyOrNumber(r.get("zeroTo30", 0))
            t_total = parseCurrencyOrNumber(r.get("totalAmount", 0))
            thr, pct, base = late_fee_params_for_prop(r.get("v2PropId", ""))
            amount = computeLateFee(t_total, z_clean, threshold=thr, percent=pct, base=base)
            if amount < 0:
                amount = 0.0
            
            charge_raw = str(r.get("chargeDateRaw","") or "").strip()
            post_raw   = str(r.get("postingDateRaw","") or "").strip()
            charge_iso = toYMD(charge_raw) or datetime.utcnow().strftime("%Y-%m-%d")
            post_iso   = toYMD(post_raw)   or datetime.utcnow().strftime("%Y-%m-%d")

            rows.append({
                "Property Name": r.get("propName",""),
                "Unit Name": r.get("unitName",""),
                "Occupancy UID": occ_uid or "",
                "Tenant Name": lastCommaFirstToFirstLast(r.get("payerName","")),
                "Occupancy ID": occ_v2,
                "Amount": round(amount, 2),
                "Charge Date": charge_raw,
                "Posting Date": post_raw,
                "Gl Account Number": TABLE_GL_ACCOUNT_NUMBER,
                 "Description": f"IL Custom Late Fee - {first_of_month_mmddyyyy_from_ymd(charge_iso)}",
                "_chargeDateV2": charge_raw, 
                "_postingDateV2": post_raw,

                "_tenantIntegrationId": integ or "",
                "_v0OccupancyId": v0OccId,
                "_v2UnitId": r.get("v2UnitId",""),
                "_v2PropertyId": r.get("v2PropId",""),
                "_chargeDateV2": r.get("chargeDate",""),
                "_postingDateV2": r.get("postingDate",""),

                "_zeroTo30": z_clean,
                "_totalAmount": t_total,
            })
        timings["map_rows"] = time.perf_counter() - t0_rows

        if any(not rr.get("_v0OccupancyId") for rr in rows):
            t0_lookup = time.perf_counter()
            try:
                integration_ids = [rr.get("_tenantIntegrationId", "") for rr in rows if not rr.get("_v0OccupancyId")]
                unit_ids = [rr.get("_v2UnitId", "") for rr in rows if not rr.get("_v0OccupancyId")]
                v0_lookup = fetchV0TenantsByIds(integration_ids, unit_ids)
                if v0_lookup:
                    tId, iId, uId = _build_v0_maps(v0_lookup)
                    _apply_v0_maps(rows, tId, iId, uId)
            except Exception as ex:
                print("v0 targeted lookup failed:", repr(ex))
                warnings.append(f"v0 targeted lookup failed: {type(ex).__name__}: {ex}")
            timings["v0_lookup"] = time.perf_counter() - t0_lookup

        if resolve_missing_flag and any(not rr.get("_v0OccupancyId") for rr in rows):
            t0_fallback = time.perf_counter()
            try:
                v0tenants_all = fetchV0TenantsAll(3650)
                tId, iId, uId = _build_v0_maps(v0tenants_all)
                _apply_v0_maps(rows, tId, iId, uId)
            except Exception as ex:
                print("wide tenant fallback failed:", repr(ex))
                warnings.append(f"v0 wide fallback failed: {type(ex).__name__}: {ex}")
            timings["v0_wide_fallback"] = time.perf_counter() - t0_fallback

        timings["total"] = time.perf_counter() - total_start
        if response is not None:
            response.headers["Server-Timing"] = _format_server_timing(timings)
        print("table_data timings:", _format_server_timing(timings))
        return {"rows": rows, "warnings": warnings}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"table-data failed: {e}")

@api.post("/bulk-charges")
def bulk_charges(body: Dict[str, Any]):
    try:
        rows = body.get("rows", [])
        if not isinstance(rows, list) or not rows:
            raise HTTPException(status_code=400, detail="No rows provided.")

        today = datetime.utcnow().date().isoformat()

        data = []
        for r in rows:
            occId = str(r.get("_v0OccupancyId") or "").strip()
            amount = float(r.get("Amount") or 0)
            chargedOn = toYMD(r.get("Charge Date") or today)  # default to today
            # default description to "Late Rent Charges - Late fee for <Mon>"
            desc = r.get("Description") or f"IL Custom Late Fee - {first_of_month_mmddyyyy_from_ymd(chargedOn)}"

            if not occId or not (amount > 0):
                # skip invalid entries (UI already warns for missing _v0OccupancyId)
                continue

            data.append({
                "AmountDue": f"{amount:.2f}",
                "ChargedOn": chargedOn,
                "Description": desc,
                "GlAccountId": BULK_GL_ACCOUNT_ID,
                "OccupancyId": occId,
                "ReferenceId": gen_uuid(),
            })

        if not data:
            raise HTTPException(status_code=400, detail="No valid rows to send. Check OccupancyId and Amount.")

        url = f"{V0_BASE}/charges/bulk"
        resp = requests.post(url, headers={
            "X-AppFolio-Developer-ID": V0_DEV_ID,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": authV0(),
        }, json={"data": data}, timeout=60)

        if not resp.ok:
            # Surface exact API response body to the UI for easier debugging
            raise HTTPException(status_code=resp.status_code, detail=f"Bulk create failed: {resp.text}")

        return resp.json()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"bulk-charges failed: {e}")


# UUID helper
def gen_uuid() -> str:
    import uuid
    return str(uuid.uuid4())

CSV_HEADERS = [
    "Property Name",
    "Unit Name",
    "Occupancy UID",
    "Tenant Name",
    "Occupancy ID",
    "Amount",
    "Charge Date",
    "Posting Date",
    "Gl Account Number",
    "Description",
]

def rows_to_csv_io(rows: list[dict]) -> io.StringIO:
    """
    Convert table rows to a CSV in-memory buffer.
    Writes a UTF-8 BOM so Excel opens it cleanly.
    """
    sio = io.StringIO()
    sio.write("\ufeff")  # Excel-friendly BOM
    w = csv.writer(sio, lineterminator="\n")
    w.writerow(CSV_HEADERS)
    for r in rows:
        w.writerow([
            r.get("Property Name", ""),
            r.get("Unit Name", ""),
            r.get("Occupancy UID", ""),
            r.get("Tenant Name", ""),
            r.get("Occupancy ID", ""),
            f'{float(r.get("Amount") or 0):.2f}',
            str(r.get("Charge Date", "") or ""),
            str(r.get("Posting Date", "") or ""),
            r.get("Gl Account Number", ""),
            r.get("Description", ""),
        ])
    sio.seek(0)
    return sio

@api.post("/export-csv")
def export_csv(body: Dict[str, Any]):
    rows = body.get("rows", [])
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="No rows provided for export.")
    sio = rows_to_csv_io(rows)                     # already writes UTF-8 BOM
    data = sio.getvalue().encode("utf-8")          # or "utf-8-sig" if you prefer
    fname = f'bulk-charges-{datetime.utcnow().date().isoformat()}.csv'
    headers = {"Content-Disposition": f'attachment; filename="{fname}"'}
    return Response(content=data, media_type="text/csv; charset=utf-8", headers=headers)

@api.post("/export-csv-ticket")
def export_csv_ticket(body: Dict[str, Any]):
    rows = body.get("rows", [])
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="No rows provided for export.")
    sio = rows_to_csv_io(rows)
    data = sio.getvalue().encode("utf-8")  # BOM already in text; fine to use utf-8
    ticket = gen_uuid()
    now = time.time()
    with _EXPORT_LOCK:
        # purge old tickets
        expired = [k for k,(ts,_) in _EXPORT_TICKETS.items() if now - ts > _EXPORT_TTL]
        for k in expired:
            _EXPORT_TICKETS.pop(k, None)
        _EXPORT_TICKETS[ticket] = (now, data)
    return {"ticket": ticket}

@api.get("/export-csv/{ticket}")
def export_csv_by_ticket(ticket: str):
    with _EXPORT_LOCK:
        tup = _EXPORT_TICKETS.pop(ticket, None)
    if not tup:
        raise HTTPException(status_code=404, detail="Expired or invalid ticket")
    _, data = tup
    fname = f'bulk-charges-{datetime.utcnow().date().isoformat()}.csv'
    headers = {"Content-Disposition": f'attachment; filename="{fname}"'}
    return Response(content=data, media_type="text/csv; charset=utf-8", headers=headers)


# Build app with router and static mount (static last)
app.include_router(api, prefix="/api")
app.mount("/", StaticFiles(directory=UI_DIR, html=True), name="ui")
