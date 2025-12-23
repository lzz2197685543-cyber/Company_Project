import time
import json
import hmac
import hashlib
import base64
import urllib.parse
import requests
import asyncio


class DingTalkBot:
    def __init__(self, access_token: str, secret: str, timeout: int = 10):
        self.access_token = access_token
        self.secret = secret
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

    async def send_text(self, message: str, at_mobiles=None, is_at_all=False):
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


ACCESS_TOKEN = '630602c35a2f4d8db5081042101eb01f9bd0a84a71bb50c0e528d3ca33d8534f'
SECRET ='SECeddb89430ef8852bbd6b58a8c40319550bfff049462fdf6f3a083ecebe164b78'

bot = DingTalkBot(ACCESS_TOKEN, SECRET)


async def main():
    await bot.send_text("你好啊")


if __name__ == "__main__":
    asyncio.run(main())







