from servies.parcel_tracer.Pickup_tracking import PickupTrace
from servies.parcel_tracer.stockin_manager import StockinManager
from utils.dingding_doc import DingTalkTokenManager, upload_multiple_records, test_delete_records
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
import asyncio
import time

logger = get_logger('shein_parcel_tracer')

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"


async def main():
    total_start = time.perf_counter()
    shop_name_list = ["希音全托301-yijia", "希音全托302-juyule", "希音全托303-kedi", "希音全托304-xiyue"]
    for shop_name in shop_name_list:
        pass
    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
    ding_bot_send('me', 'Temu揽收丢件任务结束')
