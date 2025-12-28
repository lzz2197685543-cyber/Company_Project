from modules.financial_data import TKLoginDownloadData
from utils.config_loader import get_shop_config
import asyncio
from utils.logger import get_logger
import time

logger = get_logger("financial_month_job")
"""跑tk财务"""

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"

async def main():
    total_start = time.perf_counter()
    start_time = input('请输入你要查询的月份开始日期（如：2025-11-01）：')
    end_time = input('请输入你要查询的月份结束日期（如：2025-11-30）：')
    name_list = ["TK全托1401店", "TK全托408-LXZ", "TK全托407-huidan", "TK全托406-yuedongwan", "TK全托405-huanchuang",
                 "TK全托404-kedi", "TK全托403-juyule", "TK全托401-xiyue", "TK全托402-quzhi", "TK全托1402店"]

    for name in name_list:
        account = get_shop_config(name)

        t = TKLoginDownloadData(name, account,start_time,end_time)
        await t.run()

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")


if __name__ == "__main__":
    asyncio.run(main())