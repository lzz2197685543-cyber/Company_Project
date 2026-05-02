from servies.parcel_tracer.Pickup_tracking import PickupTrace
from servies.parcel_tracer.stockin_manager import StockinManager
from servies.parcel_tracer.Pickup_tracking_l import PickupTracel
from utils.dingding_doc import DingTalkTokenManager, upload_multiple_records, test_delete_records
from utils.dingding_table import DingTalkDocClient  # 确保导入客户端类
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
from utils.webchat_send import webchat_send
from datetime import datetime
import asyncio
import time
import pymysql

logger = get_logger('shopee_parcel_tracer')

# 用于记录当天已发送预警的店铺（避免重复发送）
sent_shops_wechat = set()


# 数据库连接配置
DB_CONFIG = {
    "host": "rm-bp186omby3lautfn0no.mysql.rds.aliyuncs.com",
    "port": 3306,
    "user": "root_lxz",
    "password": "Lxz123456",
    "database": "py_spider",
    "charset": 'utf8mb4',
    "cursorclass": pymysql.cursors.DictCursor
}



# 钉钉表配置
DELIVERY_SHEET_CONFIG = {
    "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
    "sheet_id": 'temu-发货揽收轨迹及丢件',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}

STOCKIN_SHEET_CONFIG = {
    "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
    "sheet_id": 'temu-入库情况',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}

# 店铺列表
SHOP_NAME_LIST = [
    "虾皮全托1501店",
    "虾皮全托507-lxz",
    "虾皮全托506-kedi",
    "虾皮全托505-qipei",
    "虾皮全托504-huanchuang",
    "虾皮全托503-juyule",
    "虾皮全托502-xiyue",
    "虾皮全托501-quzhi"
]


def format_seconds(seconds: float) -> str:
    """格式化秒数为分秒"""
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"


def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(**DB_CONFIG)


async def delete_data():
    """删除钉钉表中的数据"""
    logger.info('----------------开始删除钉钉表中(shopee-发货揽收轨迹及丢件)的数据---------------------')
    test_delete_records(DELIVERY_SHEET_CONFIG, logger)
    logger.info('----------------开始删除钉钉表中(shopee-入库情况)的数据---------------------')
    test_delete_records(STOCKIN_SHEET_CONFIG, logger)


async def upload_delivery_data(table_data):
    """上传发货揽收轨迹及丢件数据"""
    logger.info('----------------开始上传(shopee-发货揽收轨迹及丢件)的数据---------------------')
    upload_multiple_records(DELIVERY_SHEET_CONFIG, table_data, logger)


async def upload_stockin_data(table_data):
    """上传入库情况数据"""
    logger.info('----------------开始上传(shopee-入库情况)的数据---------------------')
    upload_multiple_records(STOCKIN_SHEET_CONFIG, table_data, logger)


async def fetch_and_upload_data(shop_name, fetcher_class, table_preparer, data_type):
    """
    通用数据爬取和上传函数
    """
    logger.info(f'----------------开始爬取店铺{shop_name}--{data_type}数据---------------------')

    try:
        fetcher = fetcher_class(shop_name, 'tk_parcel_tracer')
        items = await fetcher.fetch_all_pages()

        # 更健壮的检查
        if items is None:
            logger.warning(f'店铺{shop_name}{data_type}数据返回None，将使用空列表')
            items = []
        elif not isinstance(items, list):
            logger.warning(f'店铺{shop_name}{data_type}数据返回类型为{type(items)}，尝试转换')
            try:
                items = list(items)
            except:
                items = []

        logger.info(f'爬取到 {len(items)} 条原始数据')

        # 如果有数据，打印第一条用于调试
        if items and len(items) > 0:
            logger.info(f'数据示例: {items[0]}')

        # 构造表数据并上传
        table_data = table_preparer(items)
        logger.info(f'准备上传 {len(table_data)} 条{data_type}数据')

        if table_data:
            if '发货' in data_type:
                await upload_delivery_data(table_data)
            else:
                await upload_stockin_data(table_data)
            logger.info(f'✅ {data_type}数据上传完成')
        else:
            logger.info(f'ℹ️ 没有需要上传的{data_type}数据')

        # 🔥 新增：如果是入库数据，检查差异并发送微信群消息
        if data_type == "入库情况":
            await check_and_send_wechat_for_stockin_differences(shop_name, items)

        return len(items)

    except Exception as e:
        logger.error(f'❌ 爬取店铺{shop_name}{data_type}数据失败: {e}')
        import traceback
        logger.error(traceback.format_exc())
        return 0


async def check_and_send_wechat_for_stockin_differences(shop_name, stockin_items):
    """检查入库差异，如果差异数量>=10则发送微信群消息"""
    differences = []
    for item in stockin_items:
        deliver_qty = int(item.get("送货数", 0))
        receive_qty = int(item.get("入库数", 0))
        if deliver_qty != receive_qty:
            diff = deliver_qty - receive_qty
            differences.append({
                'purchase_order_sn': item.get("入库ID", ""),
                'deliver_qty': deliver_qty,
                'receive_qty': receive_qty,
                'diff': diff,
                'receive_time': item.get("实际入库时间", ""),
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

    message = f"【Shopee入库差异预警】\n"
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
        message += f"   送货数量：{diff['deliver_qty']}  入库数量：{diff['receive_qty']}  差异：{diff['diff']}\n"
        if diff['receive_time']:
            message += f"   收货时间：{diff['receive_time']}\n"

    return message


def prepare_delivery_trace_data(items):
    """构造发货揽收轨迹表数据（收货中）"""
    records = []
    for item in items:
        record = {
            "数据抓取日期": item['数据抓取日期'],
            "店铺": item.get("店铺", ""),
            "备货单号": item.get("入库ID", ""),
            "到货数量": item.get('到货数量', ''),
            "物流轨迹": " | ".join(item.get("物流轨迹", [])) if item.get("物流轨迹") else "",
            "标记状态": item.get('标记状态', ''),
            "标记原因": item.get('标记原因', '')
        }
        records.append(record)
    return records


def prepare_delivery_trace_l_data(items):
    """构造发货揽收轨迹表数据（含包裹状态）"""
    records = []
    for item in items:
        record = {
            "数据抓取日期": item['数据抓取日期'],
            "店铺": item.get("店铺", ""),
            "备货单号": item.get("入库ID", ""),
            "包裹状态": item.get("物流状态", ""),
            "订单状态": item.get('订单状态', ''),
            "预约取货时间": item.get("创建时间", ""),
            "物流轨迹": " | ".join(item.get("物流轨迹", [])) if item.get("物流轨迹") else "",
            "标记状态": item.get('标记状态', ''),
            "标记原因": item.get('标记原因', '')
        }
        records.append(record)
    return records


def prepare_stockin_data(items):
    """构造入库情况表数据（只保留有差异的记录）"""
    records = []
    for item in items:
        # 只保留送货数量≠入库数量的记录
        if int(item.get("送货数", 0)) != int(item.get("入库数", 0)):
            record = {
                "数据爬取日期": item['数据爬取日期'],
                "店铺": item.get("店铺", ""),
                "备货单号": item.get("入库ID", ""),
                "送货数量": item.get("送货数", 0),
                "入库数量": item.get("入库数", 0),
                "收货时间": item.get("实际入库时间", "")
            }
            records.append(record)
    return records


def query_shop_abnormal_data(shop_name):
    """
    查询单个店铺的异常数据

    Returns:
        tuple: (delivery_abnormals, delivery_abnormals_l, stockin_differences)
    """
    db = None
    cursor = None
    try:
        db = get_db_connection()
        cursor = db.cursor()

        # 查询发货异常数据（表1：shopee_delivery_note_record）
        cursor.execute("""
            SELECT * FROM `shopee_delivery_note_record` 
            WHERE DATE(updated_at) = CURDATE() 
            AND shop_name = %s 
            AND mark_status != '正常'
        """, (shop_name,))
        delivery_abnormals = cursor.fetchall()

        # 查询发货异常数据（表2：shopee_delivery_note_record12）
        cursor.execute("""
            SELECT * FROM `shopee_delivery_note_record12` 
            WHERE DATE(updated_at) = CURDATE() 
            AND shop_name = %s 
            AND mark_status != '正常'
        """, (shop_name,))
        delivery_abnormals_l = cursor.fetchall()

        # 查询入库数据，并筛选差异记录
        cursor.execute("""
            SELECT * FROM `shopee_purchase_stock_record` 
            WHERE DATE(create_time) = CURDATE() 
            AND shop_name = %s
        """, (shop_name,))
        stockin_records = cursor.fetchall()

        # 筛选出入库数量≠送货数量的记录
        stockin_differences = []
        for row in stockin_records:
            deliver_qty = int(row.get('deliver_quantity', 0))
            receive_qty = int(row.get('receive_quantity', 0))
            if deliver_qty != receive_qty:
                stockin_differences.append(row)

        return delivery_abnormals, delivery_abnormals_l, stockin_differences

    except Exception as e:
        logger.error(f"查询店铺 {shop_name} 异常数据失败: {e}")
        return [], [], []

    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()


def build_shop_abnormal_message(shop_name, delivery_abnormals, delivery_abnormals_l, stockin_differences):
    """构建单个店铺的异常消息"""
    message_parts = []

    # 添加标题
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    title = f"🏪 **【{shop_name}】异常数据报告**\n报告时间: {now_str}\n"
    message_parts.append(title)

    # 统计总数
    total_delivery = len(delivery_abnormals) + len(delivery_abnormals_l)
    has_abnormal = False

    # 处理发货异常（表1）
    if delivery_abnormals:
        has_abnormal = True
        message_parts.append(build_delivery_section("发货异常-普通", delivery_abnormals))

    # 处理发货异常（表2）
    if delivery_abnormals_l:
        has_abnormal = True
        message_parts.append(build_delivery_section("发货异常-物流", delivery_abnormals_l))

    # 处理入库差异
    if stockin_differences:
        has_abnormal = True
        message_parts.append(build_stockin_section(stockin_differences))

    # 如果没有异常数据
    if not has_abnormal:
        message_parts.append("✅ 本次巡检未发现异常数据！")

    return "\n".join(message_parts)


def build_delivery_section(section_name, records):
    """构建发货异常部分的消息"""
    section = f"\n📦 **{section_name}**: {len(records)}条\n"

    # 按标记状态分类
    status_stats = {}
    for row in records:
        status = row.get('mark_status', '未知')
        status_stats[status] = status_stats.get(status, 0) + 1

    if status_stats:
        section += "   状态分类:\n"
        for status, count in status_stats.items():
            section += f"     • {status}: {count}条\n"

    # 显示详情（最多10条）
    section += "\n   📋 异常详情:\n"
    for i, row in enumerate(records):
        inbound_id = row.get('inbound_id', row.get('入库ID', '未知'))
        order_status = row.get('order_status', row.get('订单状态', '未知'))
        mark_status = row.get('mark_status', '未知')
        mark_reason = row.get('mark_reason', '')

        section += f"     {i + 1}. {inbound_id}\n"
        section += f"       状态: {order_status} -> {mark_status}\n"
        if mark_reason:
            section += f"       原因: {mark_reason[:50]}\n"

    return section


def build_stockin_section(records):
    """构建入库差异部分的消息"""
    section = f"\n📥 **入库差异**: {len(records)}条\n"

    section += "   📋 差异详情:\n"
    for i, row in enumerate(records):
        deliver_qty = int(row.get('deliver_quantity', 0))
        receive_qty = int(row.get('receive_quantity', 0))
        diff = deliver_qty - receive_qty
        po_sn = row.get('purchase_order_sn', '未知')

        section += f"     {i + 1}. {po_sn}\n"
        section += f"       送货: {deliver_qty} 入库: {receive_qty} 差异: {diff}\n"

    return section


def send_abnormal_messages(shop_data_list):
    """发送异常消息"""
    for shop_name, delivery_abnormals, delivery_abnormals_l, stockin_differences in shop_data_list:
        if delivery_abnormals or delivery_abnormals_l or stockin_differences:
            message = build_shop_abnormal_message(
                shop_name,
                delivery_abnormals,
                delivery_abnormals_l,
                stockin_differences
            )
            # 发送到钉钉群
            ding_bot_send('Shopee_Logistics_Exception_Monitor', message)
            logger.info(f"已发送 {shop_name} 的异常消息")
            time.sleep(1)  # 避免发送过快


async def main():
    total_start = time.perf_counter()

    # 第一阶段：爬取数据
    logger.info("🚀 开始第一阶段：爬取所有店铺数据")

    for shop_name in SHOP_NAME_LIST:
        try:
            # 爬取发货揽收轨迹数据（收货中）
            await fetch_and_upload_data(
                shop_name,
                PickupTrace,
                prepare_delivery_trace_data,
                "发货揽收轨迹(收货中)"
            )

            # 爬取发货揽收轨迹数据（含包裹状态）
            await fetch_and_upload_data(
                shop_name,
                PickupTracel,
                prepare_delivery_trace_l_data,
                "发货揽收轨迹(含包裹状态)"
            )

            # 爬取入库情况数据
            await fetch_and_upload_data(
                shop_name,
                StockinManager,
                prepare_stockin_data,
                "入库情况"
            )

            # 避免请求过快
            await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"处理店铺 {shop_name} 数据爬取时发生错误: {e}")
            continue

    logger.info("✅ 所有店铺数据爬取完成")

    # 第二阶段：查询异常数据
    logger.info("🚀 开始第二阶段：查询各店铺异常数据")

    shop_abnormal_data = []
    for shop_name in SHOP_NAME_LIST:
        try:
            delivery_abnormals, delivery_abnormals_l, stockin_differences = query_shop_abnormal_data(shop_name)
            shop_abnormal_data.append((
                shop_name,
                delivery_abnormals,
                delivery_abnormals_l,
                stockin_differences
            ))

            logger.info(f"已处理 {shop_name}: "
                        f"发货异常(普通){len(delivery_abnormals)}条, "
                        f"发货异常(物流){len(delivery_abnormals_l)}条, "
                        f"入库差异{len(stockin_differences)}条")

        except Exception as e:
            logger.error(f"处理店铺 {shop_name} 异常数据查询时发生错误: {e}")
            continue

    # 第三阶段：发送消息
    logger.info("🚀 开始第三阶段：发送异常消息")
    send_abnormal_messages(shop_abnormal_data)

    # 统计信息
    total_delivery = sum(len(d) + len(l) for _, d, l, _ in shop_abnormal_data)
    total_stockin = sum(len(s) for _, _, _, s in shop_abnormal_data)
    abnormal_shops = sum(1 for _, d, l, s in shop_abnormal_data if d or l or s)

    # 发送总结
    # summary_message = (
    #     f"🎯 Shopee揽收丢件任务完成\n"
    #     f"总耗时：{format_seconds(time.perf_counter() - total_start)}\n"
    #     f"处理店铺数：{len(SHOP_NAME_LIST)}个\n"
    #     f"异常店铺数：{abnormal_shops}个\n"
    #     f"总发货异常：{total_delivery}条\n"
    #     f"总入库差异：{total_stockin}条\n"
    #     f"已向每个异常店铺发送独立报告"
    # )
    #
    # ding_bot_send('me', summary_message)
    # logger.info(summary_message)

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
    ding_bot_send('me', f'Shopee揽收丢件任务结束,总耗时：{format_seconds(total_cost)}')


if __name__ == "__main__":
    sent_shops_wechat.clear()
    asyncio.run(main())