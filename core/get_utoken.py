import requests
import execjs
from pathlib import Path


JS_DIR = Path(__file__).resolve().parent.parent / "services"
TOKEN_JS_PATH = JS_DIR / "xiaozhuxiong.js"


def get_utoken(
    js_path: Path = TOKEN_JS_PATH,
    timeout: int = 10,
) -> str:
    """
    获取 Utoken

    :param js_path: xiaozhuxiong.js 路径
    :param timeout: 请求超时时间
    :return: Utoken
    """
    url = "https://mapi.toysbear.net/auth/TokenAuth/GetSystemDate"
    headers = {
        "PlatForm": "PC",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
    }

    # 1. 获取 systemCode
    resp = requests.post(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    system_code = resp.json()["result"]["item"]["systemCode"]


    # 2.获取token
    json_data = {
        'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJMaXR0bGVCZWFyIiwianRpIjoiYWU5MDIzZWUtZjQ1My00ZGU4LWJmYjEtMGVhNzZiMDMwZjI5IiwiaWF0IjoxNzY4Mjc2MzgwLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiMTg5MjkwODkyMzciLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9zeXN0ZW0iOiJQQyIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3NpZCI6IjE5YjkzYjNiLTEyMzgtNDg4Ni05MWUxLWNiMDhkZTlmMmZlOCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3NwbiI6IjdiOWQxNmQ5LTgzMTYtNGY3ZC1hZDRhLWY3YzE3NTE2YThjMSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2Fub255bW91cyI6IkhTMTYzODI1NjMzMjA2MzQxIiwiaHR0cDovL3NjaGVtYXMubWljcm9zb2Z0LmNvbS93cy8yMDA4LzA2L2lkZW50aXR5L2NsYWltcy9yb2xlIjoiU2FsZXMiLCJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL3dzLzIwMDgvMDYvaWRlbnRpdHkvY2xhaW1zL3ZlcnNpb24iOiIiLCJJc01hc3RlciI6IkZhbHNlIiwiQ2hhdFVzZXJJZCI6IjE5YjkzYjNiLTEyMzgtNDg4Ni05MWUxLWNiMDhkZTlmMmZlOCIsIkNoYXRVc2VyVG9rZW4iOiJ2MTBvcWZjQjQxL3RvTDdrVUU4djVPSjFRTlhrd0NKZVIrcFZFSDZJTmcyWHNnWTdjUzlnRjNLU0xJME1DVHV4R2t0azFMMlV6eFhZZWIwM2o1czUxdz09QGNwMmcuY24ucm9uZ25hdi5jb207Y3AyZy5jbi5yb25nY2ZnLmNvbSIsIkxvZ2luVHlwZSI6IlZlcmlmaWNhdGlvbkNvZGUiLCJVbml2ZXJzYWxUeXBlQ29kZSI6IiIsIlVzZXJUeXBlIjoiLTEiLCJDaXBoZXJ0ZXh0IjoiIiwiU3lzdGVtVGltZSI6IiIsIm5iZiI6MTc2ODI3NjM4MCwiZXhwIjoxNzY4MjgzNTgwLCJpc3MiOiJMaXR0bGVCZWFyIiwiYXVkIjoiTGl0dGxlQmVhciJ9.dFCNF3brB7n-dJztT_8GC4Q7ff9mQsT01-ftsCzaxRE',
        'screenModel': '1905x261',
        'browserVersion': '浏览器版本 = 谷歌 143.0.0.0',
        'system': 'windows',
        'systemVersion': 'Windows NT 10.0',
    }

    response = requests.post('https://api.toysbear.net/auth/api/RefreshToken', headers=headers, json=json_data)

    token=response.json()['result']['accessToken']

    # 3. 执行 JS 生成 Utoken
    with open(js_path, "r", encoding="utf-8") as f:
        js_code = f.read()

    ctx = execjs.compile(js_code)
    utoken = ctx.call("get_utoken", system_code,token)

    return utoken
