import time
from datetime import datetime

from core.base_client import HttpClient
from utils.webchat_send import webchat_send


# ================= 配置 =================
CHECK_INTERVAL = 5          # 轮询间隔（秒）
QUIET_HOURS = (23, 8)       # 夜间免打扰：23:00 ~ 08:00
# =======================================


class XiaozhuxiongMonitor:
    URL = 'https://mapi.toysbear.net/im/IMChat/GetMessageNoReadCount'

    def __init__(self):
        self.http = HttpClient()
        self.logger = self.http.logger
        self.last_count = None  # ⭐ 关键：首次启动不通知

    def is_quiet_time(self) -> bool:
        hour = datetime.now().hour
        start, end = QUIET_HOURS

        # 跨天判断
        if start < end:
            return start <= hour < end
        else:
            return hour >= start or hour < end

    def fetch(self) -> int:
        self.logger.info("正在监控---小竹熊---是否有信息")
        res = self.http.post(self.URL)
        return res["result"]["item"]["groupPlatformChat"]

    def notify(self, count: int):
        if self.is_quiet_time():
            self.logger.info("🌙 夜间免打扰，跳过通知")
            return

        msg = f"""
📬 小竹熊新消息提醒

新增未读：{count - self.last_count} 条
当前未读：{count} 条

👉 后台地址：
https://www.toysbear.com/main
        """
        webchat_send("环创-开发曾小姐", msg)
        webchat_send("环创-开发陈小姐", '有监控到消息变化')

    def run(self):
        self.logger.info("🚀 开始实时监控【小竹熊】消息")

        while True:
            try:
                count = self.fetch()
                self.logger.info(f"当前未读消息：{count}")

                # 第一次启动：只记录，不通知
                if self.last_count is None:
                    self.last_count = count
                    self.logger.info("🔰 初始化未读数，不发送通知")
                    time.sleep(CHECK_INTERVAL)
                    continue

                # 只有启动后新增才通知
                if count > self.last_count and count > 0:
                    self.notify(count)


                self.last_count = count

            except Exception as e:
                self.logger.exception(f"❌ 小竹熊监控异常：{e}")

            time.sleep(CHECK_INTERVAL)


# ================= 启动入口 =================
# if __name__ == "__main__":
#     XiaozhuxiongMonitor().run()
