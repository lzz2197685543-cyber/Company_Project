from utils.cookie_manager import CookieManager
import requests
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
import asyncio


class ShopeeBaseClient:
    def __init__(self, shop_name, job):
        self.job=job
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name,job)
        self.logger = get_logger(job)

        self.headers = {
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
        if json_data.get("__cookie_invalid__") is True:
            return True

        if "user not found" in json_data:
            return True

        if json_data.get('message')=='user not found':
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
            f"[{self.shop_name}---{self.job}]  请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[{self.shop_name}---{self.job}] 请求失败，已超过最大重试次数"
        )

    async def get(self, url: str, payload: dict, max_retry: int = 3):
        for attempt in range(1, max_retry + 1):
            try:
                cookies = await self.cookie_manager.get_auth()

                resp = requests.get(
                    url,
                    headers=self.headers,
                    cookies=cookies,
                    params=payload,
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
            f"[{self.shop_name}---{self.job}]  请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[{self.shop_name}---{self.job}] 请求失败，已超过最大重试次数"
        )
