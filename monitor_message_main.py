import asyncio
import threading

from services.monitor.xiaozhuxiong_monitor import XiaozhuxiongMonitor
from services.monitor.xiaoniaoyun_monitor_playwright import XiaoniaoyunMonitor


# ---------------- 小竹熊（同步，线程运行） ----------------
def run_xiaozhuxiong():
    monitor = XiaozhuxiongMonitor()
    monitor.run()   # ⚠️ 这是 while True


# ---------------- 宵鸟云（异步，主 loop） ----------------
async def run_xiaoniaoyun():
    monitor = XiaoniaoyunMonitor(headless=True)
    await monitor.run()   # ⚠️ 这是 async while True


# ---------------- 统一启动 ----------------
def main():
    # 2️⃣ 启动宵鸟云（主 asyncio loop）
    asyncio.run(run_xiaoniaoyun())

    # 1️⃣ 启动小竹熊（后台线程）
    t = threading.Thread(
        target=run_xiaozhuxiong,
        name="xiaozhuxiong-monitor",
        daemon=True
    )
    t.start()




if __name__ == "__main__":
    main()
