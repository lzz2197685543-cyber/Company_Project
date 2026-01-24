import requests
import time
from utils.config_loader import get_dingtalk_config


class DingTalkWorkNotifier:
    """
    钉钉工作通知（私聊 / 系统消息）
    """

    def __init__(self, timeout: int = 10):
        """
        初始化时自动读取钉钉配置
        """
        ding_cfg = get_dingtalk_config()

        self.app_key = ding_cfg["Client ID"]
        self.app_secret = ding_cfg["Client Secret"]
        self.agent_id = ding_cfg["agend_id"]

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
            raise RuntimeError(f"获取 access_token 失败: {data}")

        self._access_token = data["access_token"]
        self._expire_at = now + int(data.get("expires_in", 7200)) - 60

        return self._access_token

    # =========================
    # 发送文本消息
    # =========================
    def send_text(self, user_ids, title: str, text: str):
        """
        发送文本工作通知

        :param user_ids: list[str] | str
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

        params = {"access_token": access_token}

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
        # print(data)

        if data.get("errcode") != 0:
            raise RuntimeError(f"发送工作通知失败: {data}")

        return data


"""后面使用的话直接调用这里就可以了"""
def ding_user_send(username,title,text):
    ding_cfg = get_dingtalk_config()
    user_id = ding_cfg["userid"][username]

    notifier = DingTalkWorkNotifier()

    notifier.send_text(
        user_ids=user_id,
        title=title,
        text=text
    )



# =========================
# 测试
# =========================
# if __name__ == "__main__":
    # ding_user_send()
