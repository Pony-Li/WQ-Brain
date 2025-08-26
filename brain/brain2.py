import json
import time
import email.utils
from typing import Dict, Any, Optional, Tuple, List
from os.path import expanduser

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd


# Config
AUTH_URL = "https://api.worldquantbrain.com/authentication"
SIMULATE_URL = "https://api.worldquantbrain.com/simulations"
DATAFIELDS_URL = "https://api.worldquantbrain.com/data-fields"
ALPHA_DETAIL_URL = "https://platform.worldquantbrain.com/alpha/{alpha_id}"

REQUEST_TIMEOUT = 30          # seconds per HTTP request
DEFAULT_RETRY_SECONDS = 2.0   # seconds between polls when Retry-After missing
MAX_WAIT_SECONDS = 30 * 60    # max total polling time per simulation
REAUTH_EVERY = 100            # re-login every N simulations (0 to disable)


# sign in
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

    return sess, info


# Data Fields
def get_datafields(
    s: requests.Session,
    search_scope: Dict[str, str],
    dataset_id: str = "",
    search: str = "",
    page_limit: int = 50,
) -> pd.DataFrame:
    """
    [Fetch] Retrieve data-fields catalog for given scope.
    search_scope: {'instrumentType','region','delay','universe'} (strings)
    """
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

    # First page to get count (if available)
    r0 = s.get(DATAFIELDS_URL, params={**params_base, "offset": "0"}, timeout=REQUEST_TIMEOUT)
    if r0.status_code >= 400:
        raise RuntimeError(f"[Fetch][Error] data-fields first page failed: {r0.status_code} {r0.text}")

    j0 = r0.json()
    results = j0.get("results", [])

    # count: 满足条件的字段的总条数 (后续用来决定还需要翻几页)
    count = j0.get("count") if not search else None  # search mode may not return precise count

    # Paginate
    if count is None:
        # fallback: try a few pages conservatively when 'search' is used
        # you can increase this if needed
        max_pages = 10
        offsets = [i * page_limit for i in range(1, max_pages)]
    else:
        offsets = list(range(page_limit, int(count), page_limit))

    for off in offsets:
        r = s.get(DATAFIELDS_URL, params={**params_base, "offset": str(off)}, timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            print(f"[Fetch][Warn] page offset={off} failed: {r.status_code} {r.text}")
            break
        jr = r.json()
        page = jr.get("results", [])
        if not page:
            break
        results.extend(page)

    df = pd.DataFrame(results)
    print(f"[Fetch] data-fields rows: {len(df)}")
    return df


# Alpha Generation
def generate_alpha_list(fundamental6_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    [Alpha] Build a list of simulation payloads from fundamental6 rows (expects 'id' column).
    """
    alpha_list: List[Dict[str, Any]] = []
    for _, row in fundamental6_df.iterrows():
        field_id = str(row["id"])
        alpha_expression = f'group_rank(({field_id})/cap, subindustry)'
        payload = {
            "type": "REGULAR",
            "settings": {
                "instrumentType": "EQUITY",
                "region": "USA",
                "universe": "TOP3000",
                "delay": 1,
                "decay": 0,
                "neutralization": "INDUSTRY",
                "truncation": 0.08,
                "pasteurization": "ON",
                "unitHandling": "VERIFY",
                "nanHandling": "ON",
                "language": "FASTEXPR",
                "visualization": False,
            },
            "regular": alpha_expression,
        }
        alpha_list.append(payload)

    print(f"[Alpha] payloads prepared: {len(alpha_list)}")
    return alpha_list


# Simulation Helpers
def _parse_retry_after(headers: requests.structures.CaseInsensitiveDict) -> Optional[float]:
    """Parse Retry-After header (seconds or HTTP-date) -> waiting seconds or None."""
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)  # seconds
    except ValueError:
        try:
            dt = email.utils.parsedate_to_datetime(raw)
            return max(0.0, dt.timestamp() - time.time())
        except Exception:
            return None


def _is_done(status_code: int, body: Dict[str, Any]) -> bool:
    """Heuristics to decide if a simulation is done."""
    status = str(body.get("status") or body.get("state") or "").upper()
    if status in {"DONE", "COMPLETED", "FINISHED"}:
        return True
    if status_code == 200 and ("alpha" in body or "result" in body):
        return True
    return False


def run_simulation(sess: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    [Run] Submit simulation -> poll until done -> return final JSON body.
    """
    # submit
    resp = sess.post(SIMULATE_URL, json=payload, timeout=REQUEST_TIMEOUT)
    if resp.status_code >= 400:
        raise RuntimeError(f"[Run][Error] submit failed: {resp.status_code} {resp.text}")

    progress_url = resp.headers.get("Location")
    if not progress_url:
        raise RuntimeError(f"[Run][Error] missing 'Location' header: {resp.status_code} {resp.text}")

    print(f"[Submit] Accepted. Progress URL: {progress_url}")

    # poll
    waited = 0.0
    while True:
        r = sess.get(progress_url, timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400 and r.status_code not in (409, 425, 429):
            raise RuntimeError(f"[Run][Error] polling failed: {r.status_code} {r.text}")

        try:
            body = r.json()
        except Exception:
            body = {}

        if _is_done(r.status_code, body):
            return body

        retry = _parse_retry_after(r.headers) or DEFAULT_RETRY_SECONDS
        time.sleep(retry)
        waited += retry
        if waited > MAX_WAIT_SECONDS:
            raise TimeoutError(f"[Run][Error] polling timed out after {int(waited)}s. Last: {r.status_code} {r.text}")


def main() -> None:
    # 1) sign in
    sess, auth_info = sign_in("brain_credentials.txt")
    print("[Auth] OK.", auth_info if auth_info else "(no JSON body)")

    # 2) fetch datafields
    search_scope = {"region": "USA", "delay": "1", "universe": "TOP3000", "instrumentType": "EQUITY"}
    df = get_datafields(s=sess, search_scope=search_scope, dataset_id="fundamental6")
    print("[Info] df num of columns:", len(df.columns.tolist()))
    print("[Info] df columns:", df.columns.tolist())

    # 3) explore/inspect
    if "type" in df.columns:
        print("[Info] type counts:\n", df["type"].value_counts())

    if "type" not in df.columns or "id" not in df.columns:
        raise KeyError("[Filter][Error] required columns missing: 'type' and/or 'id'.")

    # 4) filter MATRIX only (our intended subset)
    df_matrix = df[df["type"] == "MATRIX"].copy() # 布尔索引取值
    print(f"[Filter] MATRIX rows: {len(df_matrix)}")
    print(f"[Filter] MATRIX ids: {df_matrix['id'].values}")
    
    if df_matrix.empty:
        raise ValueError("[Filter][Error] no MATRIX rows found.")

    # 5) build alpha payloads
    alpha_list = generate_alpha_list(df_matrix)

    # 6) loop simulations
    for idx, payload in enumerate(alpha_list, start=1):
        try:
            # periodic re-auth (optional)
            if REAUTH_EVERY and idx % REAUTH_EVERY == 0:
                print(f"[Auth] Re-sign in at index={idx}")
                sess, _ = sign_in("brain_credentials.txt")

            result = run_simulation(sess, payload)
            alpha_id = result.get("alpha")
            if not alpha_id:
                print(f"[Run][Warn] finished but missing 'alpha' in result at index={idx}.")
                continue

            print(f"[Alpha] #{idx} ID={alpha_id} Expr={payload['regular']}")
            print(f"[Alpha] View: {ALPHA_DETAIL_URL.format(alpha_id=alpha_id)}") # print detail URL

        except Exception as e:
            print(f"[Run][Error] index={idx} {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
