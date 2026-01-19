import asyncio
import threading

from services.monitor.xiaozhuxiong_monitor import XiaozhuxiongMonitor
from services.monitor.xiaoniaoyun_monitor_playwright import XiaoniaoyunMonitor


# ---------------- 小竹熊（同步，线程运行） ----------------
def run_xiaozhuxiong():
    monitor = XiaozhuxiongMonitor()
    monitor.run()   # while True


# ---------------- 宵鸟云（异步，主 loop） ----------------
async def run_xiaoniaoyun():
    monitor = XiaoniaoyunMonitor(headless=True)
    await monitor.run()   # async while True


def main():
    # ✅ 1. 先启动同步任务（子线程）
    t = threading.Thread(
        target=run_xiaozhuxiong,
        name="xiaozhuxiong-monitor",
        daemon=True
    )
    t.start()

    # ✅ 2. 主线程启动 asyncio（必须最后）
    asyncio.run(run_xiaoniaoyun())


if __name__ == "__main__":
    main()
