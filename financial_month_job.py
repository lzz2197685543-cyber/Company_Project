from modules.financial_data_tixian import Shein_Financial_Data_Tixian
from modules.financial_data_feiyong import Shein_Financial_Data_Feiyong
from utils.config_loader import get_shop_config
import asyncio
from utils.logger import get_logger
import time

logger = get_logger("financial_month_job")

"""跑Shein财务"""

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"

async def main():
    total_start = time.perf_counter()

    start_time = '2025-12-01'
    end_time = '2025-12-28'
    name_list = ["希音全托301-yijia", "希音全托302-juyule", "希音全托303-kedi", "希音全托304-xiyue"]
    for shop_name in name_list:
        logger.info('---------------------开始Shein提现数据的爬取-------------------')
        shein = Shein_Financial_Data_Tixian(shop_name, start_time, end_time)
        await shein.get_all_page()

        logger.info('---------------------开始Shein费用数据的爬取-------------------')
        shein = Shein_Financial_Data_Feiyong(shop_name, start_time, end_time)
        await shein.get_all_page()

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")

if __name__ == "__main__":
    asyncio.run(main())