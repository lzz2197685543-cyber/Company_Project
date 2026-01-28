from utils.cookie_manager import CookieManager
import requests
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
import asyncio

class LingXingBaseClient(object):
    def __init__(self, job):
        self.cookie_manager = CookieManager(job)
        self.logger = get_logger(job)
        self.headers = {
            'ak-origin': 'https://erp.lingxing.com',
            'auth-token': '84b0YxY4pkNtFzfPDWQz/r6r1rRuLl1j9vL5DAtNhcyqlThscCtkLMoZgh+WDdqnhVtJl0gieiEo1ZPoX1CWzztkWJUeH0b1dEhmxrPcV8IOPLZHnYN8OyP1PWf/wAIpvwm4TJEU5MT0BoLrVu9H2UsMntKFldsTzA',
            'content-type': 'application/json;charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'x-ak-company-id': '901130820149137920',
            'x-ak-request-id': 'ff0fa19f-6ac6-4158-bd74-e2c42f41c429',
            'x-ak-request-source': 'erp',
            'x-ak-uid': '11020589',
            'x-ak-version': '3.7.6.3.0.060',
            'x-ak-zid': '194665',
        }
        self.shop_id_name = {
            "293": "BAKAM账号-UK",
            "294": "BAKAM账号-US",
            "2557": "Kidzbuddy账号-UK",
            "2373": "Kidzbuddy账号-US",
            "3425": "Meemazi-UK",
            "3422": "Meemazi-US",
            "3410": "Ninigai-CA",
            "3412": "Ninigai-UK",
            "3409": "Ninigai-US",
            "1480": "YYDeek账号-UK",
            "297": "YYDeek账号-US",
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

        error_msg = json_data.get("msg")

        if '鉴权失败' in error_msg:
            return True

        return False

    async def post(self, url, payload, max_retry=3):
        for attempt in range(1, max_retry + 1):
            try:
                cookies = await self.cookie_manager.get_auth()
                self.headers["auth-token"]=cookies['auth-token']
                resp = requests.post(url, data=payload, headers=self.headers)
                print(resp.text[:200])
                resp.raise_for_status()

                data = resp.json()

                # 🔴 登录态 / cookie 失效判断
                if self.is_cookie_invalid(data):
                    raise PermissionError("cookie 已失效或接口返回异常")

                return data  # ✅ 只有“确认正常”才返回

            except PermissionError:
                self.logger.warning(
                    f"[领星ERP-跨境电商管理系统] 登录失效，刷新 cookie（第 {attempt} 次）"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(
                    f"[领星ERP-跨境电商管理系统] 请求失败（第 {attempt} 次）: {e}"
                )
                await self.cookie_manager.refresh()
                await asyncio.sleep(2)

                # ❌ 超过最大重试次数

        ding_bot_send(
            'me',
            f"[领星ERP-跨境电商管理系统] temu_site 请求失败，已超过最大重试次数"
        )
        raise RuntimeError(
            f"[领星ERP-跨境电商管理系统] 请求失败，已超过最大重试次数"
        )
