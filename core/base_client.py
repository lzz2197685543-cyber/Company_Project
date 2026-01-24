from utils.cookie_manager import CookieManager
import requests
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
import asyncio


class TemuBaseClient:
    def __init__(self, shop_name: str,job:str):
        self.shop_name = shop_name
        self.cookie_manager = CookieManager(shop_name)
        self.logger = get_logger(job)

        self.headers = {
            'mallid': '634418216684033',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

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
        error_code = json_data.get("error_code") or json_data.get("errorCode")
        error_msg = json_data.get("error_msg") or json_data.get("errorMsg")

        if error_code == 40001:
            self.logger.error('获取到的cookie是无效的，需要重新登录')
            return True

        if error_msg in (
                "Invalid Login State",
                "登录过期，请重新登录",
        ):
            return True

        return False


    async def post(self, url: str, payload: dict, max_retry: int = 3,cookie_domain:str="agentseller"):
        for attempt in range(1, max_retry + 1):
            try:
                cookies, shop_id = await self.cookie_manager.get_auth()
                self.headers["mallid"] = str(shop_id)

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
                await self.cookie_manager.refresh(cookie_domain)
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(
                    f"[{self.shop_name}] 请求失败（第 {attempt} 次）: {e}"
                )
                await self.cookie_manager.refresh(cookie_domain)
                await asyncio.sleep(2)

        # ❌ 超过最大重试次数
        ding_bot_send(
            'me',
            f"[{self.shop_name}] temu_site 请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[{self.shop_name}] 请求失败，已超过最大重试次数"
        )

