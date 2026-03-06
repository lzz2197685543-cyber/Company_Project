from services.financial.financial_data import Temu_Financial_Data
import asyncio
import time
from utils.logger import get_logger
from datetime import datetime
from utils.config_loader import  get_shop_config
from pathlib import Path
from services.financial.financial_process_up import financial_process_up
FINANCIAL_DIR = Path(__file__).resolve().parent/ "data" / "financial"

from utils.dingtalk_bot import ding_bot_send
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

async def main_all():
    month_str = get_prev_month_from_now()
    # month_str = '2025-11'
    logger.info(f'--------------------------正在下载{month_str}的数据------------------------------')
    shop_name_list = [
         "2106-Temu全托管",  "2103-Temu全托管","2102-Temu全托管", "2101-Temu全托管KA",
        "112-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管", "1105-Temu全托管", "1104-Temu全托管",
        "1103-Temu全托管", "1102-Temu全托管", "1101-Temu全托管",
        "110-Temu全托管KA", "109-Temu全托管KA", "108-Temu全托管",  "106-Temu全托管", "105-Temu全托管",
        "104-Temu全托管", "103-Temu全托管", "102-Temu全托管", "101-Temu全托管",
    ]
    # shop_name_list=["2101-Temu全托管KA"]


    # 顺序处理
    for shop_name in shop_name_list:
        logger.info(f'--------------正在爬取店铺{shop_name}----的数据')
        account = get_shop_config(shop_name)
        t = Temu_Financial_Data(shop_name, account, month_str,'financial_data')
        await t.run()

        logger.info(f'--------------正在爬取店铺{shop_name}----卖家中心的数据')



    logger.info(f'--------------------------正在处理数据------------------------------')
    # 处理数据
    filepath = FINANCIAL_DIR / f"{month_str.split('-')[1]}月份"
    financial_process_up(filepath,f"{month_str.split('-')[0]}年{month_str.split('-')[1]}月")



if __name__ == '__main__':
    total_start = time.perf_counter()
    asyncio.run(main_all())
    total_cost = time.perf_counter() - total_start
    ding_bot_send('me',f'temu_financial任务完成，总耗时：{format_seconds(total_cost)}')
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")



# 如果群里收到哪个门店三次登录失败，说明那个门店没有下载成功

# 未检测到可导出数据

#  部分财务数据下载失败


