import requests
from core.get_utoken import get_utoken
from utils.cookie_manager import CookieManager
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
import asyncio
import json
import re

"""小竹熊的基类"""
class HttpClient:
    def __init__(self):
        self.base_headers = {
            # "Content-Type": "application/json",
            "PlatForm": "PC",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "Utoken": '',
        }
        self.logger = get_logger('search_factory')

    def post(self,url,*,json=None,files=None):
        headers = self.base_headers
        headers['Utoken'] = get_utoken()

        resp = requests.post(url, headers=headers, json=json, files=files)
        print(resp.text[:200])

        if resp.status_code == 401:
            headers['Utoken'] = get_utoken()
            resp = requests.post(url, headers=headers, json=json, files=files)

        resp.raise_for_status()
        return resp.json()


"""宜采的基类"""
class YiCaiClient:
    def __init__(self):
        self.cookies = None

        self.cookies_manager=CookieManager()
        self.logger=get_logger('search_factory')

        self.headers = {
            'Referer': 'https://www.essabuy.com/toys/search-by-image?fid=40685520&req_id=176835391066824968&region=39,357,38,345&cutImg=39,357,38,345',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

    async def get(self,url,max_retry=3):
        for attempt in range(1, max_retry + 1):
            try:
                cookies= await self.cookies_manager.get_auth()
                self.cookies = cookies
                response = requests.get(
                    url=url,
                    cookies=self.cookies,
                    headers=self.headers,
                )
                # print(response.text[:200])
                res_data = re.findall(r'window._MODELS_ =(.*?)</script>', response.text, re.S)[0].strip('\n')[:-1]

                json_data = json.loads(res_data)

                if not json_data:
                    raise PermissionError("cookie 已失效或接口返回异常")

                return json_data


            except PermissionError:
                self.logger.warning(
                    f"[YiCai] 登录失效，刷新 cookie（第 {attempt} 次）"
                )
                await self.cookies_manager.refresh()
                await asyncio.sleep(2)

            except Exception as e:
                self.logger.error(
                    f"[YiCai] 请求失败（第 {attempt} 次）: {e}"
                )
                await self.cookies_manager.refresh()
                await asyncio.sleep(2)

        # ❌ 超过最大重试次数
        ding_bot_send('me',f"[YiCai] temu_site 请求失败，已超过最大重试次数")
        raise RuntimeError(f"[YiCai] 请求失败，已超过最大重试次数")
