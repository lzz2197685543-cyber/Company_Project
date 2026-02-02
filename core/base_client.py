from utils.cookie_manager import CookieManager
import requests
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
import asyncio


class SheinBaseClient:
    def __init__(self, shop_name, job):
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.logger = get_logger(job)

        self.headers = {
            'accept': 'application/json',
            'accept-language': 'zh-CN,zh;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://sso.geiwohuo.com',
            'origin-url': 'https://sso.geiwohuo.com/#/pfmp/order-management/new-order?auth_login_token=eb4fa7c2f188499e990639cda9a40b48',
            'priority': 'u=1, i',
            'referer': 'https://sso.geiwohuo.com/',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'uber-trace-id': 'fffb68fba38f5f3b:fffb68fba38f5f3b:0000000000000000:0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """
        # 请求异常
        if not json_data:
            return True

        # get_info 主动标记
        if json_data.get("msg") == "子系统登录重定向":
            return True

        if not isinstance(json_data, dict):
            return True

        return False

    async def post(self, url: str, payload: dict, max_retry: int = 3):
        for attempt in range(1, max_retry + 1):
            try:
                cookies = await self.cookie_manager.get_auth()

                resp = requests.post(
                    url,
                    headers=self.headers,
                    cookies=cookies,
                    json=payload,
                    timeout=15,
                )
                print(resp.text[:200])
                resp.raise_for_status()

                data = resp.json()

                # 🔴 登录态 / cookie 失效判断
                if self.is_cookie_invalid(data):
                    raise PermissionError("cookie 已失效或接口返回异常")

                return data  # ✅ 只有“确认正常”才返回

            except PermissionError:
                self.logger.warning(
                    f"[{self.shop_name}] 登录失效，刷新 cookie（第 {attempt} 次）"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(
                    f"[{self.shop_name}] 请求失败（第 {attempt} 次）: {e}"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

        # ❌ 超过最大重试次数
        ding_bot_send(
            'me',
            f"[{self.shop_name}]  请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[{self.shop_name}] 请求失败，已超过最大重试次数"
        )
