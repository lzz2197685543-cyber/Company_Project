from modules.temu_violation_recored import Temu_ViolationRecored
from modules.funds_restriction import Temu_Funds_Restriction

from modules.send_data import run_send_data
from modules.upload_data import run_upload_data
from utils.dingtalk_bot import ding_bot_send

import asyncio
import time
from utils.logger import get_logger

logger = get_logger("temu_risk_daily_job")

"""跑违规记录与金额限制"""


def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"


async def crawl_all_shops():
    shop_name_list = [
        "2106-Temu全托管", "2105-Temu全托管", "2108-Temu全托管",
        "2107-Temu全托管", "2102-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管",
        "1105-Temu全托管", "2103-Temu全托管",
        "112-Temu全托管", "151-Temu全托管家居",
        "1104-Temu全托管", "1102-Temu全托管",
        "1103-Temu全托管", "1101-Temu全托管",
        "2101-Temu全托管KA", "110-Temu全托管KA",
        "109-Temu全托管KA", "108-Temu全托管",
        "107-Temu全托管", "106-Temu全托管",
        "105-Temu全托管", "104-Temu全托管",
        "103-Temu全托管", "102-Temu全托管",
        "101-Temu全托管"
    ]
    # shop_name_list=[ "102-Temu全托管",
    #         "101-Temu全托管"]

    for shop_name in shop_name_list:
        logger.info(f"🚀 开始爬取店铺：{shop_name}")

        t_vio = Temu_ViolationRecored(shop_name)
        await t_vio.run()

        t_fund = Temu_Funds_Restriction(shop_name)
        await t_fund.run()


async def main():
    total_start = time.perf_counter()

    # ========= ① 爬虫 =========
    await crawl_all_shops()

    # ========= ② 上传钉钉表 =========
    logger.info("📊 开始上传钉钉多维表")
    run_upload_data()

    # ========= ③ 告警 =========
    logger.info("🔔 开始执行违规 & 资金限制告警")
    run_send_data()

    ding_bot_send('me','temu的资金限制任务完成')

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")

#
if __name__ == "__main__":
    asyncio.run(main())
