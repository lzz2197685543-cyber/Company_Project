import requests

from utils.cookie_manager import CookieManager
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
import asyncio


class SellerSpriteClient:
    def __init__(self, job):
        self.cookie_manager = CookieManager(job)
        self.logger = get_logger(job)
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        }
        self.job=job

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """
        # 请求异常
        if not json_data:
            return True

        if json_data.get('data')==None:
            return True

        if '登陆已失效' in json_data:
            return True

        if not isinstance(json_data, dict):
            return True

        return False

    async def post(self,url:str,payload:dict,max_retry:int=3):
        for attempt in range(1,max_retry+1):
            try:
                cookies=await self.cookie_manager.get_auth()

                resp=requests.post(
                    url,
                    json=payload,
                    cookies=cookies,
                    headers=self.headers,
                    timeout=15
                )
                print(resp.text[:200])
                resp.raise_for_status()

                data=resp.json()

                # 🔴 登录态 / cookie 失效判断
                if self.is_cookie_invalid(data):
                    raise PermissionError("cookie 已失效或接口返回异常")

                return data  # ✅ 只有“确认正常”才返回

            except PermissionError:
                self.logger.warning(
                    f"[卖家精灵] 登录失效，刷新 cookie（第 {attempt} 次）"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(
                    f"[卖家精灵] 请求失败（第 {attempt} 次）: {e}"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

        # ❌ 超过最大重试次数
        ding_bot_send(
            'me',
            f"[卖家精灵---{self.job}]  请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[卖家精灵---{self.job}] 请求失败，已超过最大重试次数"
        )

    async def get(self,url:str,params:dict,max_retry:int=3):
        for attempt in range(1,max_retry+1):
            try:
                cookies=await self.cookie_manager.get_auth()

                resp=requests.get(
                    url,
                    params=params,
                    cookies=cookies,
                    headers=self.headers,
                    timeout=15
                )
                print(resp.text[:200])
                resp.raise_for_status()

                data=resp.json()

                # 🔴 登录态 / cookie 失效判断
                if self.is_cookie_invalid(data):
                    raise PermissionError("cookie 已失效或接口返回异常")

                return data  # ✅ 只有“确认正常”才返回

            except PermissionError:
                self.logger.warning(
                    f"[卖家精灵] 登录失效，刷新 cookie（第 {attempt} 次）"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(
                    f"[卖家精灵] 请求失败（第 {attempt} 次）: {e}"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

        # ❌ 超过最大重试次数
        ding_bot_send(
            'me',
            f"[卖家精灵---{self.job}]  请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[卖家精灵---{self.job}] 请求失败，已超过最大重试次数"
        )
