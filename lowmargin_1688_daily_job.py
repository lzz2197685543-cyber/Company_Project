from services.lowmargin_1688.LowMargin_System_1688 import get_yesterday, format_low_margin_report, LowMarginSystem_1688
from datetime import datetime, timedelta
from utils.logger import get_logger
import asyncio
from utils.webchat_send import webchat_send
from utils.dingtalk_bot import ding_bot_send  # 导入钉钉发送函数

if __name__ == '__main__':
    shop_id_list = {
        "嬉游记": "12426494",
        "俏娃": "12430140"
    }

    # 标记是否有成功的店铺
    has_successful_shop = False
    # 记录失败信息
    failure_messages = []

    for name, id in shop_id_list.items():
        l = LowMarginSystem_1688(id, name, 'lowmargin_1688')
        low_items = asyncio.run(l.get_all_page())

        # 判断是否成功获取到数据
        if l.data_fetched_successfully:
            has_successful_shop = True
            message = format_low_margin_report(
                low_items=low_items,
                shop_name=name,
                date_str=get_yesterday()
            )
            # 发送到企业微信
            webchat_send(message, name)
        else:
            failure_messages.append(f"{name}: 登录失败或无法获取数据")

    # 如果没有一个店铺成功，发送钉钉通知
    if not has_successful_shop:
        date_str = get_yesterday()
        message = f"⚠️ 1688低利润数据获取失败告警\n\n"
        message += f"日期: {date_str}\n"
        message += "失败详情:\n"
        for msg in failure_messages:
            message += f"- {msg}\n"
        message += "\n请检查登录状态或系统配置！"

        # 发送钉钉通知
        ding_bot_send('me', message)
        print(f"\n{message}")  # 同时在控制台输出