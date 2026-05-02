from services.parcel_tracer.stockin_manager import StockinManager
from utils.dingding_doc import DingTalkTokenManager, upload_multiple_records, test_delete_records
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
from datetime import datetime
import asyncio
import time
import pymysql
from utils.webchat_send import webchat_send

logger=get_logger('tk_parcel_tracer')

# 用于记录当天已发送预警的店铺（避免重复发送）
sent_shops_wechat = set()


db = pymysql.connect(
    host='rm-bp186omby3lautfn0no.mysql.rds.aliyuncs.com',
    port=3306,
    user='root_lxz',
    password='Lxz123456',
    database='py_spider',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

config_delivery = {
    "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
    "sheet_id": 'temu-发货揽收轨迹及丢件',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}

config_stockin = {
    "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
    "sheet_id": 'temu-入库情况',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"

async def up_stockin_data(table_data):
    """上传入库情况数据"""
    logger.info('----------------开始上传(smt-入库情况)的数据---------------------')
    upload_multiple_records(config_stockin, table_data, logger)

async def fetch_and_upload_stockin_data(shop_name):
    """爬取并上传入库情况数据"""
    logger.info(f'----------------开始爬取店铺{shop_name}--入库的数据---------------------')
    s_stockin = StockinManager(shop_name, 'tk_parcel_tracer')
    stockin_items = await s_stockin.fetch_all_pages()

    # 修复：检查返回值是否为None
    if stockin_items is None:
        logger.warning(f'店铺{shop_name}入库数据返回None，将使用空列表')
        stockin_items = []

    # 构造表数据
    table_data = prepare_stockin_table_data(stockin_items)

    # 立即上传数据
    await up_stockin_data(table_data)

    # 🔥 新增：检查入库差异并发送微信群消息
    # await check_and_send_wechat_for_stockin_differences(shop_name, stockin_items)

    return len(stockin_items)


async def check_and_send_wechat_for_stockin_differences(shop_name, stockin_items):
    """检查入库差异，如果差异数量>=10则发送微信群消息"""
    differences = []
    for item in stockin_items:
        deliver_qty = int(item.get("发货数量", 0))
        receive_qty = int(item.get("总收货数量", 0))
        if deliver_qty != receive_qty:
            diff = deliver_qty - receive_qty
            differences.append({
                'purchase_order_sn': item.get("备货单号", ""),
                'deliver_qty': deliver_qty,
                'receive_qty': receive_qty,
                'diff': diff,
                'data_date': item.get('数据爬取日期', '')
            })

    # 如果差异数量大于等于10
    if len(differences) >= 10:
        # 检查今天是否已经发送过该店铺的预警
        today = datetime.now().strftime('%Y-%m-%d')
        shop_key = f"{shop_name}_{today}"

        if shop_key not in sent_shops_wechat:
            logger.info(f"⚠️ 店铺 {shop_name} 入库差异数量达到 {len(differences)} 条，准备发送微信群消息")

            # 构建微信群消息
            message = build_wechat_stockin_message(shop_name, differences)

            # 发送到微信群
            contacts = [("全托部入库异常通知群", message),
                        ('梁祖珍', f'店铺{shop_name}异常数据已经发送')]

            try:
                # webchat_send 是同步函数，在线程池中运行
                await asyncio.to_thread(webchat_send, contacts)
                sent_shops_wechat.add(shop_key)
                logger.info(f"✅ 已发送 {shop_name} 入库差异预警到微信群")
            except Exception as e:
                logger.error(f"发送微信群消息失败: {e}")
        else:
            logger.info(f"⚠️ 店铺 {shop_name} 今日已发送过预警，跳过")


def build_wechat_stockin_message(shop_name, differences):
    """构建发送到微信群的入库差异消息"""
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    message = f"【SMT入库差异预警】\n"
    message += f"店铺：{shop_name}\n"
    message += f"时间：{current_time}\n"
    message += f"差异数量：{len(differences)} 条（≥10条，触发预警）\n"
    message += f"{'=' * 30}\n\n"

    # 统计差异总量
    total_diff = sum(d['diff'] for d in differences)
    message += f"差异统计：总差异数量 {total_diff} 件\n\n"

    # 显示前10条差异详情（避免消息过长）
    show_count = min(100, len(differences))
    message += f"差异详情（前{show_count}条）：\n"

    for i, diff in enumerate(differences[:], 1):
        message += f"\n{i}. 备货单号：{diff['purchase_order_sn']}\n"
        message += f"   发货数量：{diff['deliver_qty']}  收货数量：{diff['receive_qty']}  差异：{diff['diff']}\n"



    return message

def prepare_stockin_table_data(stockin_items):
    """构造第二个钉钉表数据（入库差异表）"""
    records = []
    for item in stockin_items:
        record = {
            "数据爬取日期": item['数据爬取日期'],
            "店铺": item.get("店铺", ""),
            "备货单号": item.get("备货单号", ""),
            "送货数量": item.get("发货数量", 0),
            "入库数量": item.get("总收货数量", 0),
        }
        records.append(record)
    return records


def query_shop_abnormal_data_from_db(shop_name):
    """查询单个门店的异常数据"""
    try:
        cursor = db.cursor()

        # 查询该门店的入库异常数据
        cursor.execute("""
            SELECT * FROM `smt_purchase_stock_record` 
            WHERE DATE(create_time) = CURDATE() 
            AND shop_name = %s
        """, (shop_name,))
        stockin_abnormals = cursor.fetchall()

        cursor.close()

        return stockin_abnormals

    except Exception as e:
        logger.error(f"查询店铺 {shop_name} 异常数据失败: {e}")
        return [], []


def build_shop_abnormal_message(shop_name, stockin_abnormals):
    """构建单个门店的异常消息"""
    message_parts = []


    # 添加标题
    title = f"🏪 **【{shop_name}】异常数据报告**\n"
    title += f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    message_parts.append(title)

    # 入库异常部分
    if stockin_abnormals:
        stockin_summary = f"\n📥 **入库差异**: {len(stockin_abnormals)}条\n"

        # 详细列表（最多显示5条）
        stockin_summary += "   📋 差异详情:\n"
        for i, row in enumerate(stockin_abnormals[:]):
            deliver_qty = int(row.get('deliver_quantity', 0))
            receive_qty = int(row.get('receive_quantity', 0))
            listing_qty = int(row.get('listing_quantity', 0))

            stockin_summary += f"     {i + 1}.备货单号： {row['purchase_order_sn']}\n"
            stockin_summary += f"       发货数量: {deliver_qty} 总收货数量: {receive_qty} 总上架数量: {listing_qty}  \n"

        message_parts.append(stockin_summary)

    # 如果没有异常数据
    if  not stockin_abnormals:
        message_parts.append("✅ 本次巡检未发现异常数据！")

    return "\n".join(message_parts)


def send_shop_messages(shop_data):
    """发送单个门店的消息"""
    for shop_name, stockin_count, message in shop_data:
        if "未发现异常数据" not in message:
            # 发送到钉钉群
            ding_bot_send('SMT_Logistics_Exception_Monitor', message)
            # ding_bot_send('me', message)
            logger.info(f"已发送 {shop_name} 的异常消息")
            # 添加短暂延迟，避免发送过快
            time.sleep(1)


async def main():
    total_start = time.perf_counter()

    shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']

    # 1. 第一阶段：爬取所有店铺数据并保存到数据库
    logger.info("🚀 开始第一阶段：爬取所有店铺数据、保存到数据库并将数据上传到钉钉表中")
    for shop_name in shop_name_list:

        # 爬取并上传入库情况数据
        stockin_count = await fetch_and_upload_stockin_data(shop_name)
        logger.info(f'{shop_name} 入库情况数据爬取完成，共{stockin_count}条记录')

        # 添加延迟避免请求过快
        await asyncio.sleep(2)

    logger.info("✅ 所有店铺数据爬取完成")

    # # 2. 第二阶段：按门店查询异常数据并发送消息
    logger.info("🚀 开始第二阶段：按门店查询异常数据并发送消息")

    shop_data = []
    for shop_name in shop_name_list:

        try:
            stockin_abnormals = query_shop_abnormal_data_from_db(shop_name)
            message = build_shop_abnormal_message(shop_name,  stockin_abnormals)

            # 统计入库差异数量
            stockin_count = len(stockin_abnormals)

            shop_data.append((shop_name, stockin_count, message))
            logger.info(f"已处理 {shop_name}: 入库差异{stockin_count}条")

        except Exception as e:
            logger.error(f"处理店铺 {shop_name} 时发生错误: {e}")
            # 现在即使出错，stockin_differences 也已经定义
            continue

    # 3. 发送所有门店的消息
    logger.info("🚀 开始第三阶段：发送各门店异常消息")
    send_shop_messages(shop_data)

    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(time.perf_counter() - total_start)}")
    ding_bot_send('me', f'SMT揽收丢件任务结束,总耗时：{format_seconds(time.perf_counter() - total_start)}')


if __name__ == "__main__":
    sent_shops_wechat.clear()
    asyncio.run(main())