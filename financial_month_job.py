from modules.financial_data import Temu_Financial_Data
import asyncio
import time
from utils.logger import get_logger

"""跑temu财务数据"""

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"

logger = get_logger("financial_month_job")

if __name__ == '__main__':
    total_start = time.perf_counter()

    start_time=input('请输入你要查询的月份开始日期（如：2025-11-01）：')
    end_time=input('请输入你要查询的月份结束日期（如：2025-11-30）：')
    shop_name_list = [
        "2106-Temu全托管", "2105-Temu全托管", "2108-Temu全托管","2107-Temu全托管", "2102-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管","1105-Temu全托管", "2103-Temu全托管",
        "112-Temu全托管", "151-Temu全托管家居","1104-Temu全托管", "1102-Temu全托管",
        "1103-Temu全托管", "1101-Temu全托管","2101-Temu全托管KA", "110-Temu全托管KA",
        "109-Temu全托管KA", "108-Temu全托管","107-Temu全托管", "106-Temu全托管",
        "105-Temu全托管", "104-Temu全托管","103-Temu全托管", "102-Temu全托管","101-Temu全托管",
    ]
    for shop_name in shop_name_list:
        t=Temu_Financial_Data(shop_name,start_time,end_time)
        asyncio.run(t.run())

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")