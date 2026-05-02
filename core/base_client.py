from utils.cookie_manager import CookieManager
import requests
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
from utils.cookie_manager import CookieManager
import asyncio
import aiohttp
import json


class BaseClient:
    def __init__(self, job: str):
        self.logger = get_logger(job)

        self.headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            # "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://bi.erp321.com",
            "Referer": "https://bi.erp321.com/app/daas/report/subject/adsfinance/detail.aspx?r=0.5585252346454567&___skutype=combinesku",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": "\"Google Chrome\";v=\"147\", \"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"147\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }

        self.cookie_manager = CookieManager(job)

    def is_cookie_invalid(self, json_data):
        """
        只判断真正的登录失效。
        解析失败不等于 cookie 失效。
        """
        if not isinstance(json_data, dict):
            return False

        if json_data.get("__invalid_cookie__"):
            return True

        if json_data.get("msg") in ["子系统登录重定向", "token验证失败!"]:
            return True

        return False

    def safe_load_json(self, res_text):
        """
        解析聚水潭 BI 接口返回值。

        常见返回格式：
        0|{"IsSuccess":true,"ExceptionMessage":null,"ReturnValue":"{\"dp\":...,\"datas\":[...]}"}

        返回：
        - 正常：ReturnValue 里面的 dict
        - 登录失效：{"__invalid_cookie__": True, "raw": 原文}
        - 解析失败：{"__parse_error__": True, "raw": 原文, "error": 错误}
        """
        if res_text is None:
            return {"__parse_error__": True, "raw": "", "error": "empty response"}

        raw = str(res_text).strip()

        if not raw:
            return {"__parse_error__": True, "raw": raw, "error": "empty response"}

        # 有些接口前面带 0|
        if raw.startswith("0|"):
            raw = raw[2:]

        try:
            outer = json.loads(raw)
        except json.JSONDecodeError as e:
            self.logger.error(f"外层 JSON 解析失败: {e}")
            self.logger.error(f"原始响应前500字符: {raw[:500]}")
            return {
                "__parse_error__": True,
                "raw": raw,
                "error": str(e),
            }

        # 外层明确失败
        if isinstance(outer, dict) and outer.get("IsSuccess") is False:
            return {
                "__invalid_cookie__": True,
                "raw": outer,
                "error": outer.get("ExceptionMessage"),
            }

        return_value = outer.get("ReturnValue")

        if return_value in (None, "", "{}"):
            return {"ReturnValue": "{}"}

        # ReturnValue 正常是字符串
        if isinstance(return_value, dict):
            return return_value

        try:
            return json.loads(return_value)
        except json.JSONDecodeError as e:
            self.logger.error(f"ReturnValue JSON 解析失败: {e}")
            self.logger.error(f"ReturnValue前500字符: {str(return_value)[:500]}")
            return {
                "__parse_error__": True,
                "raw": return_value,
                "error": str(e),
            }

    async def post(self, url: str, payload: dict=None, params: dict = None, max_retry: int = 3):
        for attempt in range(1, max_retry + 1):
            try:
                cookies = await self.cookie_manager.get_auth()
                timeout = aiohttp.ClientTimeout(total=15)

                async with aiohttp.ClientSession(cookies=cookies, headers=self.headers, timeout=timeout) as session:
                    async with session.post(url, data=payload, params=params) as resp:
                        resp.raise_for_status()
                        data = await resp.text()

                        print('响应信息:', data[:200])

                        json_data = self.safe_load_json(data)
                        # print('14输出的响应信息：',str(data)[:200])

                        # 🔴 登录态 / cookie 失效判断
                        if self.is_cookie_invalid(json_data):
                            raise PermissionError("cookie 已失效或接口返回异常")

                        return json_data  # ✅ 只有“确认正常”才返回

            except PermissionError:
                self.logger.warning(
                    f"[聚水潭] 登录失效，刷新 cookie（第 {attempt} 次）"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(
                    f"[聚水潭] 请求失败（第 {attempt} 次）: {e}"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

        # ❌ 超过最大重试次数
        ding_bot_send(
            'me',
            f"[聚水潭] 请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[聚水潭] 请求失败，已超过最大重试次数"
        )

    async def post_sync(self, url: str, payload: dict=None, params: dict = None, max_retry: int = 3):
        """使用 requests 库的同步请求（推荐用于有复杂cookie验证的场景）"""
        for attempt in range(1, max_retry + 1):
            try:
                cookies = await self.cookie_manager.get_auth()

                response=requests.post(
                    url=url,
                    params=params,
                    cookies=cookies,
                    headers=self.headers,
                    json=payload,
                    timeout=15
                )

                json_data = response.json()
                print('响应信息:', str(json_data)[:200])

                # 登录态失效判断
                if self.is_cookie_invalid(json_data):
                    raise PermissionError("cookie 已失效或接口返回异常")

                return json_data

            except PermissionError:
                self.logger.warning(f"[聚水潭] 登录失效，刷新 cookie（第 {attempt} 次）")
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)
            except Exception as e:
                self.logger.error(f"[聚水潭] 请求失败（第 {attempt} 次）: {e}")
                await asyncio.sleep(2)

        ding_bot_send('me', f"[聚水潭] 请求失败，已超过最大重试次数")
        raise RuntimeError(f"[聚水潭] 请求失败，已超过最大重试次数")


    async def get(self, url: str,params: dict = None, max_retry: int = 3):
        """使用 requests 库的同步请求（推荐用于有复杂cookie验证的场景）"""
        for attempt in range(1, max_retry + 1):
            try:
                cookies = await self.cookie_manager.get_auth()

                response=requests.get(
                    url=url,
                    params=params,
                    cookies=cookies,
                    headers=self.headers,
                    timeout=15
                )

                json_data = response.text
                print('响应信息:', str(json_data)[:200])

                # 登录态失效判断
                if response.status_code != 200:
                    raise PermissionError("cookie 已失效或接口返回异常")

                return json_data

            except PermissionError:
                self.logger.warning(f"[聚水潭] 登录失效，刷新 cookie（第 {attempt} 次）")
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)
            except Exception as e:
                self.logger.error(f"[聚水潭] 请求失败（第 {attempt} 次）: {e}")
                await asyncio.sleep(2)

        ding_bot_send('me', f"[聚水潭] 请求失败，已超过最大重试次数")
        raise RuntimeError(f"[聚水潭] 请求失败，已超过最大重试次数")