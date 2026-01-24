import requests
import asyncio
import json
from typing import Dict, Any

"""获取hub指纹浏览器的环境"""


BASE_URL = "http://127.0.0.1:6873"


# ---------------- 获取账号密码 ----------------
async def get_environment_password(params: Dict[str, Any]) -> Dict[str, str]:
    """
    查询环境绑定账号的明文密码
    """
    url = f"{BASE_URL}/api/v1/account/list"

    resp = requests.post(url, json=params, timeout=10)
    data = resp.json()

    account_list = data.get("data", {}).get("list", [])
    if not account_list:
        raise RuntimeError(f"账号列表为空: {params}")

    # 防止 HubStudio 接口抖动
    await asyncio.sleep(0.2)

    return {
        "accountName": account_list[0]["accountName"],
        "password": account_list[0]["accountPassword"],
    }


# ---------------- 查询环境 ----------------
async def query_environment_id(tag_name: str) -> Dict[str, Any]:
    """
    根据环境标签查询 HubStudio 环境，并展开账号 & 代理信息
    """
    url = f"{BASE_URL}/api/v1/env/list"

    payload = {
        "tagNames": [tag_name],
        "current": 1,
        "size": 200
    }

    resp = requests.post(url, json=payload, timeout=10)
    data = resp.json()

    env_list = data.get("data", {}).get("list", [])
    result = {"environments": []}

    for item in env_list:
        accounts = item.get("accounts", [])
        if not accounts:
            continue

        account = accounts[0]

        env_password = await get_environment_password({
            "accountName": account["accountName"],
            "name": account["name"],
            "current": 1,
            "size": 100
        })

        result["environments"].append({
            "id": item.get("serialNumber"),
            "name": item.get("containerName"),
            "hubId": item.get("containerCode"),
            "credentials": {
                "username": env_password["accountName"],
                "password": env_password["password"],
            },
            "proxy": {
                "host": item.get("proxyHost"),
                "port": item.get("proxyPort"),
                "type": "socks5",
                "username": item.get("proxyAccount"),
                "password": item.get("proxyPassword"),
            }
        })

    return result


# ---------------- 主入口 ----------------
async def main():
    envs = await query_environment_id("TEMU全托管")

    # 保存为 config.json
    with open("config.json", "w", encoding="utf-8") as f:
        json.dump(envs, f, ensure_ascii=False, indent=2)

    print("✅ 环境配置已保存到 config.json")

    # 控制台简单输出确认
    for env in envs["environments"]:
        print(
            env["name"],
            env["hubId"],
            env["credentials"]["username"]
        )


if __name__ == "__main__":
    asyncio.run(main())
