import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth

# 加载凭据文件
with open(expanduser('brain_credentials.txt')) as f:
    credentials = json.load(f)

# 从列表中提取用户名和密码
username, password = credentials

# 创建会话对象
sess = requests.Session()

# 设置基本身份验证
sess.auth = HTTPBasicAuth(username, password)

# 向API发送POST请求进行身份验证
response = sess.post('https://api.worldquantbrain.com/authentication')

# 打印响应状态和内容以调试
print(response.status_code)
print(response.json())

simulation_data = {
    'type': 'REGULAR',
    'settings': {
        'instrumentType': 'EQUITY',
        'region': 'USA',
        'universe': 'TOP3000',
        'delay': 1,
        'decay': 0,
        'neutralization': 'INDUSTRY',
        'truncation': 0.08,
        'pasteurization': 'ON',
        'unitHandling': 'VERIFY',
        'nanHandling': 'OFF',
        'language': 'FASTEXPR',
        'visualization': False,
    },
    'regular': 'liabilities/assets'  ## 写表达式
}

from time import sleep

sim_resp = sess.post(
    'https://api.worldquantbrain.com/simulations',
    json=simulation_data,
) # 服务器并不会立刻给你仿真的最终结果，而是返回一个响应，其中 HTTP 响应头里有一个 Location 字段


# 从提交仿真的响应头里取出 Location, 其中通常含有一个进度查询/结果查询的 URL, 后续就去这个 URL 轮询状态
sim_progress_url = sim_resp.headers['Location']

while True:
    sim_progress_resp = sess.get(sim_progress_url) # 向进度地址发起 GET 请求, 获取当前任务状态
    retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0)) # 只要 get("Retry-After") 的返回值不是 0 就说明仍在模拟中, 需要等待
    if retry_after_sec == 0:  # simulation done!模拟完成!
        break
    sleep(retry_after_sec) # 按照 get("Retry-After") 的返回值等待一段时间

alpha_id = sim_progress_resp.json()["alpha"]  # the final simulation result 模拟最终模拟结果

print(alpha_id) # 可以在 https://platform.worldquantbrain.com/alpha/{alpha_id} 查询 alpha 的详细信息
