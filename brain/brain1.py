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
)

sim_progress_url = sim_resp.headers['Location']

while True:
    sim_progress_resp = sess.get(sim_progress_url)
    retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))
    if retry_after_sec == 0:  # simulation done!模拟完成!
        break
    sleep(retry_after_sec)

alpha_id = sim_progress_resp.json()["alpha"]  # the final simulation result 模拟最终模拟结果

print(alpha_id)
