from modules.financial_data import SMT_FinancialData
from utils.logger import get_logger
from utils.config_loader import get_shop_config
import asyncio
import time
from datetime import datetime


logger = get_logger(f"financial_data")

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
    logger.info('程序开始启动')
    shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
    month_str=get_prev_month_from_now()
    logger.info(f'正在下载{month_str}的数据')
    for name in shop_name_list:
        t = SMT_FinancialData(name,month_str)
        await t.run()

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")


if __name__ == "__main__":
    asyncio.run(main())