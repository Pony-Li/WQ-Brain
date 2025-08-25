import json
import time
import email.utils
from typing import Dict, Any, Optional, Tuple
from os.path import expanduser
import requests
from requests.auth import HTTPBasicAuth

AUTH_URL = "https://api.worldquantbrain.com/authentication"
SIMULATE_URL = "https://api.worldquantbrain.com/simulations"
ALPHA_DETAIL_URL = "https://platform.worldquantbrain.com/alpha/{alpha_id}"
REQUEST_TIMEOUT = 30
DEFAULT_RETRY_SECONDS = 2.0
MAX_WAIT_SECONDS = 30 * 60


def sign_in(credentials_path: str = 'brain_credentials.txt') -> Tuple[requests.Session, Dict[str, Any]]:

    """加载凭据 -> 建 Session -> 基本认证 -> 触发认证端点"""

    with open(expanduser(credentials_path), "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, (list, tuple)) and len(data) == 2:
        username, password = map(str, data)
    elif isinstance(data, dict) and "username" in data and "password" in data:
        username, password = str(data["username"]), str(data["password"])
    else:
        raise ValueError("Credentials file must be ['user','pass'] or {'username':...,'password':...}")

    sess = requests.Session() # 创建会话对象
    sess.auth = HTTPBasicAuth(username, password) # 设置基本身份认证
    resp = sess.post(AUTH_URL, timeout=REQUEST_TIMEOUT) # 向 API 发送 POST 请求进行身份验证

    if resp.status_code >= 400: # 如果身份验证失败, 则抛出错误
        raise RuntimeError(f"Authentication failed: {resp.status_code} {resp.text}")

    try:
        return sess, resp.json() # 返回创建的会话对象和相应内容
    except Exception:
        return sess, {}


def _parse_retry_after(headers: requests.structures.CaseInsensitiveDict) -> Optional[float]:
    
    """解析 HTTP 头部的 Retry-After 字段, 返回等待秒数"""

    raw = headers.get("Retry-After")

    if raw is None:
        return None
    
    try:
        return float(raw)  # 秒数
    except ValueError:
        try:
            # email.utils.parsedate_to_datetime() 是 Python 内置的一个工具函数, 可以把符合 RFC 2822 格式的日期字符串解析成 datetime 对象
            dt = email.utils.parsedate_to_datetime(raw) # 日期时间字符串
            return max(0.0, dt.timestamp() - time.time()) # 从现在到目标时间的剩余秒数
        except Exception:
            return None


def _is_done(status_code: int, body: Dict[str, Any]) -> bool:

    """检查仿真是否完成"""

    status = str(body.get("status") or body.get("state") or "").upper()
    if status in {"DONE", "COMPLETED", "FINISHED"}:
        return True
    if status_code == 200 and ("alpha" in body or "result" in body):
        return True
    return False


def run_simulation(sess: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:

    """
    提交仿真 + 轮询直到完成, 返回最终 JSON 结果
    """

    # 1) 提交仿真
    resp = sess.post(SIMULATE_URL, json=payload, timeout=REQUEST_TIMEOUT) # 服务器并不会立刻给你仿真的最终结果而是返回一个响应, 其中 HTTP 响应头里有一个 Location 字段

    if resp.status_code >= 400: # 如果提交失败, 则抛出错误
        raise RuntimeError(f"Submit failed: {resp.status_code} {resp.text}")

    progress_url = resp.headers.get("Location") # 从提交仿真的响应头里取出 Location, 其中通常含有一个进度/结果查询的 URL, 后续就去这个 URL 轮询状态
    if not progress_url: # 如果没有 Location 头部, 则抛出错误
        raise RuntimeError(f"No 'Location' header. Response: {resp.status_code} {resp.text}")

    print(f"[Submit] Accepted. Progress URL: {progress_url}") # 打印进度 URL

    # 2) 轮询进度
    waited = 0.0
    while True:

        r = sess.get(progress_url, timeout=REQUEST_TIMEOUT) # 使用轮询进度 URL 获取当前进度
        if r.status_code >= 400 and r.status_code not in (409, 425, 429): # 如果轮询失败, 则抛出错误
            raise RuntimeError(f"Polling failed: {r.status_code} {r.text}")

        try:
            body = r.json()
        except Exception:
            body = {}

        if _is_done(r.status_code, body): # 如果仿真完成, 则返回结果
            return body

        retry = _parse_retry_after(r.headers) or DEFAULT_RETRY_SECONDS
        time.sleep(retry)
        waited += retry

        if waited > MAX_WAIT_SECONDS:
            raise TimeoutError(f"Polling timed out after {int(waited)}s. Last: {r.status_code} {r.text}")


def main() -> None:
    # 登录
    sess, auth_info = sign_in('brain_credentials.txt')
    print("[Auth] OK.", auth_info if auth_info else "(no JSON body)")

    # 仿真参数
    simulation_data = {
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
            "nanHandling": "OFF",
            "language": "FASTEXPR",
            "visualization": False,
        },
        "regular": "liabilities/assets",
    }

    # 运行仿真
    result = run_simulation(sess, simulation_data)
    print("[Result] JSON:", result)

    alpha_id = result.get("alpha")
    if not alpha_id:
        raise RuntimeError(f"No 'alpha' in final result: {result}")

    print(f"[Alpha] ID: {alpha_id}")
    print(f"[Alpha] View: {ALPHA_DETAIL_URL.format(alpha_id=alpha_id)}") # 使用该 URL 可以在浏览器中查看该 Alpha 的详细信息


if __name__ == "__main__":
    main()
