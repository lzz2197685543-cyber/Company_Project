from utils.cookie_manager import CookieManager
import requests
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
from utils.cookie_manager import CookieManager
import asyncio
import aiohttp


class BaseClient:
    def __init__(self, job: str):
        self.logger = get_logger(job)

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
        }
        self.cookie_manager=CookieManager(job)

    def is_cookie_invalid(self, json_data) -> bool:
        """
        统一判断 cookie / 登录态是否失效
        True  = 失效
        False = 正常
        """
        # 请求异常 / 无返回
        if not json_data:
            return True

        if not isinstance(json_data, dict):
            return True

        # 常见登录失效返回（site / 财务）
        error_code = json_data.get("code") or json_data.get("errorCode")
        error_msg = json_data.get("error_msg") or json_data.get("errorMsg")

        if error_code == 50001:
            self.logger.error('获取到的cookie是无效的，需要重新登录')
            return True

        return False

    async def post(self, url: str, payload: dict, max_retry: int = 3):
        for attempt in range(1, max_retry + 1):
            try:
                cookies = await self.cookie_manager.get_auth()
                timeout=aiohttp.ClientTimeout(total=15)

                async with aiohttp.ClientSession(cookies=cookies,headers=self.headers,timeout=timeout) as session:
                    async with session.post(url,data=payload) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                        # print('14输出的响应信息：',str(data)[:200])

                        # 🔴 登录态 / cookie 失效判断
                        if self.is_cookie_invalid(data):
                            raise PermissionError("cookie 已失效或接口返回异常")

                        return data  # ✅ 只有“确认正常”才返回

            except PermissionError:
                self.logger.warning(
                    f"[妙手] 登录失效，刷新 cookie（第 {attempt} 次）"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(
                    f"[妙手] 请求失败（第 {attempt} 次）: {e}"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

        # ❌ 超过最大重试次数
        ding_bot_send(
            'me',
            f"[妙手] 请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[妙手] 请求失败，已超过最大重试次数"
        )
