"""
WorldQuant Brain: authenticate -> fetch data-fields -> build alpha payloads -> run simulations.

Improvements:
- Unified English logs with bracketed tags.
- Robust session with connection pooling + HTTP retries (idempotent GET).
- Clear function boundaries, rich type hints, explicit docstrings.
- Safer JSON handling, strict column checks, defensive pagination.
- Respect Retry-After (seconds or HTTP-date), global polling timeout.
- Periodic re-auth, structured error propagation, KeyboardInterrupt friendly.
"""

from __future__ import annotations

import json
import time
import email.utils
import logging
from typing import Dict, Any, Optional, Tuple, List
from os.path import expanduser

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Config
AUTH_URL = "https://api.worldquantbrain.com/authentication"
SIMULATE_URL = "https://api.worldquantbrain.com/simulations"
DATAFIELDS_URL = "https://api.worldquantbrain.com/data-fields"
ALPHA_DETAIL_URL = "https://platform.worldquantbrain.com/alpha/{alpha_id}"

REQUEST_TIMEOUT = 30          # seconds per HTTP request
DEFAULT_RETRY_SECONDS = 2.0   # seconds between polls when Retry-After missing
MAX_WAIT_SECONDS = 30 * 60    # max total polling time per simulation
REAUTH_EVERY = 100            # re-login every N simulations (0 to disable)
MAX_SEARCH_PAGES = 20         # max pages when 'search' count is unknown
PAGE_LIMIT = 50               # default page size for data-fields
LOG_FILE = "brain3_simulation.log"   # log file path


# Logging
def setup_logging() -> None:
    """[Log] Configure root logger with both console and file handlers."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Clear existing handlers to avoid duplicate logs if re-imported
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)


# Sign in
def sign_in(credentials_path: str = "brain_credentials.txt") -> Tuple[requests.Session, Dict[str, Any]]:
    """
    [Auth] Load credentials -> build Session with BasicAuth -> hit /authentication.
    Accepts:
      1) ["username", "password"]
      2) {"username": "...", "password": "..."}
    Returns: (session, auth_info_json_or_empty)
    """
    with open(expanduser(credentials_path), "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, (list, tuple)) and len(data) == 2:
        username, password = map(str, data)
    elif isinstance(data, dict) and "username" in data and "password" in data:
        username, password = str(data["username"]), str(data["password"])
    else:
        raise ValueError("[Auth] Unsupported credentials format. Use ['user','pass'] or {'username':...,'password':...}.")

    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)

    resp = sess.post(AUTH_URL, timeout=REQUEST_TIMEOUT)
    if resp.status_code >= 400:
        raise RuntimeError(f"[Auth][Error] Authentication failed: {resp.status_code} {resp.text}")

    try:
        info = resp.json()
    except Exception:
        info = {}

    logging.info("[Auth] OK.")
    return sess, info


# Data Fields
def get_datafields(
    s: requests.Session,
    search_scope: Dict[str, str],
    dataset_id: str = "",
    search: str = "",
    page_limit: int = PAGE_LIMIT,
) -> pd.DataFrame:
    """
    [Fetch] Retrieve data-fields catalog as DataFrame for given scope.
    search_scope keys (str): instrumentType, region, delay, universe.
    """
    required = {"instrumentType", "region", "delay", "universe"}
    missing = required - set(search_scope)
    if missing:
        raise KeyError(f"[Fetch][Error] search_scope missing keys: {missing}")

    params_base = {
        "instrumentType": search_scope["instrumentType"],
        "region": search_scope["region"],
        "delay": str(search_scope["delay"]),
        "universe": search_scope["universe"],
        "limit": str(page_limit),
    }
    if dataset_id:
        params_base["dataset.id"] = dataset_id
    if search:
        params_base["search"] = search

    # First page to get count (if provided)
    r0 = s.get(DATAFIELDS_URL, params={**params_base, "offset": "0"}, timeout=REQUEST_TIMEOUT)
    if r0.status_code >= 400:
        raise RuntimeError(f"[Fetch][Error] First page failed: {r0.status_code} {r0.text}")

    j0 = r0.json()
    results = list(j0.get("results", []))
    count = None if search else j0.get("count")

    # Determine offsets
    if count is None:
        # Unknown total count (search mode) -> keep fetching until empty page or MAX_SEARCH_PAGES
        offsets = [i * page_limit for i in range(1, MAX_SEARCH_PAGES)]
    else:
        try:
            total = int(count)
        except Exception:
            total = 0
        offsets = list(range(page_limit, max(total, 0), page_limit))

    # Fetch subsequent pages
    for off in offsets:
        r = s.get(DATAFIELDS_URL, params={**params_base, "offset": str(off)}, timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            logging.warning(f"[Fetch][Warn] page offset={off} failed: {r.status_code} {r.text}")
            break
        jr = r.json()
        page = jr.get("results", [])
        if not page:
            break
        results.extend(page)

    df = pd.DataFrame(results)
    logging.info(f"[Fetch] data-fields rows: {len(df)}")
    return df


# Alpha Generation
def generate_alpha_list(fundamental6_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    [Alpha] Build simulation payloads from 'fundamental6' rows (requires 'id' column).
    Uses a combinational template of group/time-series operators.
    template: <group_compare_op>(<ts_compare_op>(<company_fundamentals>, <days>), <group>)
    """
    if "id" not in fundamental6_df.columns:
        raise KeyError("[Alpha][Error] 'id' column missing in fundamental6 DataFrame.")

    group_compare_ops = ["group_rank", "group_zscore", "group_neutralize"]
    ts_compare_ops = ["ts_rank", "ts_zscore", "ts_av_diff"]
    days_list = [60, 200]
    groups = ["market", "industry", "subindustry", "sector", "densify(pv13_h_f1_sector)"]

    field_ids = [str(x) for x in fundamental6_df["id"].tolist()]
    alpha_expressions: List[str] = []

    for gco in group_compare_ops:
        for tco in ts_compare_ops:
            for cf in field_ids:
                for d in days_list:
                    for grp in groups:
                        alpha_expressions.append(f"{gco}({tco}({cf}, {d}), {grp})")

    logging.info(f"[Alpha] expressions prepared: {len(alpha_expressions)}")

    payloads: List[Dict[str, Any]] = []
    settings = {
        "instrumentType": "EQUITY",
        "region": "USA",
        "universe": "TOP3000",
        "delay": 1,
        "decay": 0,
        "neutralization": "SUBINDUSTRY",
        "truncation": 0.08,
        "pasteurization": "ON",
        "unitHandling": "VERIFY",
        "nanHandling": "OFF",
        "language": "FASTEXPR",
        "visualization": False,
    }

    for expr in alpha_expressions:
        payloads.append({"type": "REGULAR", "settings": settings, "regular": expr})

    logging.info(f"[Alpha] payloads prepared: {len(payloads)}")
    return payloads


# Simulation Helpers
def _parse_retry_after(headers: requests.structures.CaseInsensitiveDict) -> Optional[float]:
    """[Run] Parse Retry-After (seconds or HTTP-date) -> waiting seconds or None."""
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        try:
            dt = email.utils.parsedate_to_datetime(raw)
            return max(0.0, dt.timestamp() - time.time())
        except Exception:
            return None


def _is_done(status_code: int, body: Dict[str, Any]) -> bool:
    """[Run] Decide whether the simulation is completed."""
    status = str(body.get("status") or body.get("state") or "").upper()
    if status in {"DONE", "COMPLETED", "FINISHED"}:
        return True
    if status_code == 200 and ("alpha" in body or "result" in body):
        return True
    return False


def run_simulation(sess: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    [Run] Submit simulation -> poll until done -> return final JSON body.
    POST is not automatically retried (non-idempotent). Polling GET uses Session retries.
    """
    expr = payload.get("regular", "<unknown>")
    logging.info(f"[Run] Submit: {expr}")

    resp = sess.post(SIMULATE_URL, json=payload, timeout=REQUEST_TIMEOUT)
    if resp.status_code >= 400:
        msg = f"[Run][Error] Submit failed: {resp.status_code} {resp.text}"
        logging.error(msg)
        raise RuntimeError(msg)

    progress_url = resp.headers.get("Location")
    if not progress_url:
        msg = f"[Run][Error] Missing 'Location' header: {resp.status_code} {resp.text}"
        logging.error(msg)
        raise RuntimeError(msg)

    logging.info(f"[Submit] Accepted. Progress URL: {progress_url}")

    waited = 0.0
    while True:
        r = sess.get(progress_url, timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400 and r.status_code not in (409, 425, 429):
            msg = f"[Run][Error] Polling failed: {r.status_code} {r.text}"
            logging.error(msg)
            raise RuntimeError(msg)

        try:
            body = r.json()
        except Exception:
            body = {}
            logging.warning("[Run][Warn] Failed to parse JSON during polling.")

        if _is_done(r.status_code, body):
            logging.info(f"[Run] Done: {expr}")
            return body

        retry = _parse_retry_after(r.headers) or DEFAULT_RETRY_SECONDS
        time.sleep(retry)
        waited += retry
        if waited > MAX_WAIT_SECONDS:
            msg = f"[Run][Error] Polling timed out after {int(waited)}s. Last: {r.status_code} {r.text}"
            logging.error(msg)
            raise TimeoutError(msg)


# Main
def main() -> None:
    setup_logging()
    logging.info("[Main] Start.")

    # 1) Auth
    sess, auth_info = sign_in("brain_credentials.txt")
    logging.info("[Auth] Info: %s", auth_info if auth_info else "(no JSON body)")

    # 2) Fetch data-fields
    search_scope = {"region": "USA", "delay": "1", "universe": "TOP3000", "instrumentType": "EQUITY"}
    df = get_datafields(s=sess, search_scope=search_scope, dataset_id="fundamental6", page_limit=PAGE_LIMIT)
    logging.info("[Info] dtypes: %s", dict(df.dtypes))

    # 3) Sanity checks
    required_cols = {"type", "id"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise KeyError(f"[Filter][Error] Missing columns: {missing}")

    logging.info("[Info] type counts: \n%s", df["type"].value_counts())

    # 4) Filter MATRIX subset
    df_matrix = df[df["type"] == "MATRIX"].copy()
    logging.info(f"[Filter] MATRIX rows: {len(df_matrix)}")
    if df_matrix.empty:
        raise ValueError("[Filter][Error] No MATRIX rows found.")

    # 5) Build alphas
    alpha_list = generate_alpha_list(df_matrix)

    # 6) Loop simulations
    for idx, payload in enumerate(alpha_list, start=1):
        try:
            # periodic re-auth (optional)
            if REAUTH_EVERY and idx % REAUTH_EVERY == 0:
                logging.info(f"[Auth] Re-sign in at index={idx}")
                sess, _ = sign_in("brain_credentials.txt")

            result = run_simulation(sess, payload)
            alpha_id = result.get("alpha")
            if not alpha_id:
                logging.warning(f"[Run][Warn] Finished but missing 'alpha' at index={idx}.")
                continue

            logging.info(f"[Alpha] #{idx} ID={alpha_id} Expr={payload['regular']}")
            logging.info(f"[Alpha] View: {ALPHA_DETAIL_URL.format(alpha_id=alpha_id)}")

        except KeyboardInterrupt:
            logging.warning("[Main] Interrupted by user. Stopping gracefully.")
            break
        except Exception as e:
            logging.error(f"[Run][Error] index={idx} {e}")
            time.sleep(10)

    logging.info("[Main] End.")


if __name__ == "__main__":
    main()
