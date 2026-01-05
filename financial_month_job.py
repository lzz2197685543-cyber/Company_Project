from modules.financial_data import Temu_Financial_Data
import asyncio
import time
from utils.logger import get_logger
from datetime import datetime
from utils.config_loader import  get_shop_config
from pathlib import Path
from modules.financial_process_up import financial_process_up

FINANCIAL_DIR = Path(__file__).resolve().parent.parent / "data" / "financial"
logger = get_logger("financial_data")

"""跑temu财务数据"""

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"



# 计算前一个月的年份和月份
def get_prev_month_from_now() -> str:
    """
    返回当前时间的前一个月，格式：YYYY-MM
    """
    now = datetime.now()
    year = now.year
    month = now.month

    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1

    return f"{year}-{month:02d}"

if __name__ == '__main__':
    total_start = time.perf_counter()

    # month_str = get_prev_month_from_now()
    month_str='2025-11'
    logger.info(f'--------------------------正在下载{month_str}的数据------------------------------')
    shop_name_list = [
       "2108-Temu全托管","2107-Temu全托管", "2106-Temu全托管", "2105-Temu全托管",  "2103-Temu全托管","2102-Temu全托管","2101-Temu全托管KA",
        "112-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管","1105-Temu全托管","1104-Temu全托管","1103-Temu全托管", "1102-Temu全托管","1101-Temu全托管",
        "110-Temu全托管KA","109-Temu全托管KA", "108-Temu全托管","107-Temu全托管", "106-Temu全托管","105-Temu全托管", "104-Temu全托管","103-Temu全托管", "102-Temu全托管","101-Temu全托管",
    ]
    for shop_name in shop_name_list:
        account = get_shop_config(shop_name)
        t=Temu_Financial_Data(shop_name,account,month_str)
        asyncio.run(t.run())


    logger.info(f'----------------------------开始处理数据---------------------------------')
    filepath=FINANCIAL_DIR/month_str.split("-")[1]+'月份'
    financial_process_up(filepath)

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")


# 未检测到可导出数据
"""1107\(跳出去了） 1101(跳出去了）"""

#  部分财务数据下载失败
