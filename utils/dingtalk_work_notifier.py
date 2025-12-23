import requests
import time


class DingTalkWorkNotifier:
    """
    钉钉工作通知（私聊 / 系统消息）
    """
    def __init__(self, app_key: str, app_secret: str, agent_id: int, timeout: int = 10):
        self.app_key = app_key
        self.app_secret = app_secret
        self.agent_id = agent_id
        self.timeout = timeout

        self._access_token = None
        self._expire_at = 0  # token 过期时间戳

    # =========================
    # Token 管理
    # =========================
    def _get_access_token(self):
        """
        获取并缓存 access_token（官方有效期 2 小时）
        """
        now = int(time.time())
        if self._access_token and now < self._expire_at:
            return self._access_token

        url = "https://oapi.dingtalk.com/gettoken"
        params = {
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }

        resp = requests.get(url, params=params, timeout=self.timeout)
        data = resp.json()

        if data.get("errcode") != 0:
            raise Exception(f"获取 access_token 失败: {data}")

        self._access_token = data["access_token"]
        # 官方 expires_in = 7200
        self._expire_at = now + int(data.get("expires_in", 7200)) - 60

        return self._access_token

    # =========================
    # 核心发送方法
    # =========================
    def send_text(self, user_ids, title: str, text: str):
        """
        发送文本工作通知

        :param user_ids: list[str]  钉钉 userid 列表
        :param title: 消息标题
        :param text: 消息内容
        """
        if isinstance(user_ids, str):
            user_ids = [user_ids]

        access_token = self._get_access_token()

        url = (
            "https://oapi.dingtalk.com/topapi/message/"
            "corpconversation/asyncsend_v2"
        )

        params = {
            "access_token": access_token
        }

        body = {
            "agent_id": self.agent_id,
            "userid_list": ",".join(user_ids),
            "msg": {
                "msgtype": "text",
                "text": {
                    "content": f"{title}\n{text}"
                }
            }
        }

        resp = requests.post(
            url,
            params=params,
            json=body,
            timeout=self.timeout
        )

        data = resp.json()
        print(data)

        if data.get("errcode") != 0:
            raise Exception(f"发送工作通知失败: {data}")

        return data

if __name__ == '__main__':
    APP_KEY = "dings13rpbdzmpis6dyo"
    APP_SECRET = "J0gloUMrhko4ca_Esar9jiVtef8vT-Qd5AJq9B3zYeyR7pEdoIHibaUFU8NZfz9o"
    AGENT_ID = 4036386083
    USER_IDS = ["106246005536840537"]  # 钉钉 userid

    notifier = DingTalkWorkNotifier(
        app_key=APP_KEY,
        app_secret=APP_SECRET,
        agent_id=AGENT_ID
    )

    notifier.send_text(
        user_ids=USER_IDS,
        title="库存异常",
        text="SKU123 当前库存为 0，请立即处理"
    )

    print("工作通知发送成功")
