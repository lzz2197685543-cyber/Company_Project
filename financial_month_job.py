from servies.financial.financial_data import ShopeeLogin_FinancialData
from utils.logger import get_logger
from utils.config_loader import get_shop_config
import asyncio
import time
from datetime import datetime
from utils.dingtalk_bot import ding_bot_send

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
    name_list = ["虾皮全托1501店", "虾皮全托507-lxz","虾皮全托506-kedi", "虾皮全托505-qipei",
                 "虾皮全托504-huanchuang","虾皮全托503-juyule","虾皮全托502-xiyue","虾皮全托501-quzhi"]

    # name_list=["虾皮全托501-quzhi"]


    month_str = get_prev_month_from_now()
    logger.info(f'正在下载{month_str}的数据')

    for name in name_list:
        account = get_shop_config(name)

        t = ShopeeLogin_FinancialData(name, account,month_str)
        await t.run()

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
    ding_bot_send('me', f'Shopee的财务任务完成,总耗时：{format_seconds(total_cost)}')


if __name__ == "__main__":
    asyncio.run(main())