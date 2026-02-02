from servies.financial.financial_data_tixian import Shein_Financial_Data_Tixian
from servies.financial.financial_data_feiyong import Shein_Financial_Data_Feiyong
import asyncio
from utils.logger import get_logger
import time
from datetime import datetime

logger = get_logger("financial_data")

"""跑Shein财务"""

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

async def main():
    total_start = time.perf_counter()

    month_str = get_prev_month_from_now()
    logger.info(f'正在下载{month_str}的数据')
    name_list = ["希音全托301-yijia", "希音全托302-juyule", "希音全托303-kedi", "希音全托304-xiyue"]
    for shop_name in name_list:
        logger.info('---------------------开始Shein提现数据的爬取-------------------')
        shein = Shein_Financial_Data_Tixian(shop_name, month_str)
        await shein.get_all_page()

        logger.info('---------------------开始Shein费用数据的爬取-------------------')
        shein = Shein_Financial_Data_Feiyong(shop_name, month_str)
        await shein.get_all_page()

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")

if __name__ == "__main__":
    asyncio.run(main())