import time
import json
import hmac
import hashlib
import base64
import urllib.parse
import requests
import asyncio
from utils.config_loader import get_dingtalk_config

"""给群发信息"""
class DingTalkBot:
    def __init__(self, group_name,timeout: int = 10):
        ding_cfg = get_dingtalk_config()
        self.access_token = ding_cfg['bots'][group_name]['access_token']
        self.secret = ding_cfg['bots'][group_name]['secret']
        self.timeout = timeout

    def _build_signed_url(self):
        """
        构造带签名的钉钉机器人 URL
        """
        ts = int(time.time() * 1000)  # 毫秒时间戳
        string_to_sign = f"{ts}\n{self.secret}"

        hmac_code = hmac.new(
            self.secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()

        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))

        url = (
            "https://oapi.dingtalk.com/robot/send"
            f"?access_token={self.access_token}&timestamp={ts}&sign={sign}"
        )

        return url

    def _post(self, body: dict):
        """
        发送 POST 请求
        """
        url = self._build_signed_url()

        headers = {
            "Content-Type": "application/json"
        }

        resp = requests.post(
            url,
            headers=headers,
            data=json.dumps(body),
            timeout=self.timeout
        )

        try:
            data = resp.json()
        except Exception:
            data = resp.text

        return resp.status_code, data

    def send_text(self, message: str, at_mobiles=None, is_at_all=False):
        """
        发送文本消息
        """
        if at_mobiles is None:
            at_mobiles = []

        body = {
            "msgtype": "text",
            "text": {
                "content": message
            },
            "at": {
                "atMobiles": at_mobiles,
                "isAtAll": is_at_all
            }
        }

        status, data = self._post(body)

        if 200 <= status < 300:
            return data
        else:
            raise Exception(f"HTTP {status}: {data}")


def ding_bot_send(group_name,text):
    bot = DingTalkBot(group_name)
    bot.send_text(text)


# if __name__ == "__main__":
    # ding_bot_send('me','平安夜快乐')







