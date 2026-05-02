from core.login import DianLeiDaLogin
from api.filter_automation import OfferFilterAutomation
from api.offer_api_capture import OfferApiCapture
from utils.page_helpers import close_popup_if_exists, close_btn_if_exists, close_popup_if_exists3
import asyncio
import random
from storage.ali1688_monitor_storage import Ali1688MonitorStorage
from monitor.ali1688_change_detector import Ali1688ChangeDetector
from utils.dingtalk_bot import ding_bot_send
from utils.logger import get_logger
from utils.dingding_doc import upload_multiple_records, DingTalkTokenManager, DingTalkSheetQuery
import time
from typing import List, Dict, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
config = {
    "base_id": "gvNG4YZ7JnwebGnESqKO3r6182LD0oRE",
    "sheet_id": 'XPcSO7O',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}

config_sku = {
    "base_id": "gvNG4YZ7JnwebGnESqKO3r6182LD0oRE",
    "sheet_id": 'thKJnnX',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}



def test_query_records():
    """获取需要监控的商品ID列表"""
    config = {
        "base_id": "gvNG4YZ7JnwebGnESqKO3r6182LD0oRE",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE",
        "sheet_id": "8p426HR"
    }

    token_manager = DingTalkTokenManager()

    query = DingTalkSheetQuery(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    all_records = query.get_all_records(batch_size=50)

    items = []

    for record in all_records:
        fields = record.get("fields", {})
        product_name = fields.get("产品分组")
        link_info = fields.get("产品链接（只能复制到html)", {})
        url = link_info.get("link")

        if url and "1688" in url:
            item = url.split('.html')[0].split('/')[-1]
            items.append(item)

    return '、'.join(items)


def format_seconds(seconds: float) -> str:
    """格式化时间"""
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"

def convert_decimal_to_native(obj):
    """递归转换 Decimal 类型为原生类型"""
    if isinstance(obj, Decimal):
        # 如果是整数，转换为 int，否则转换为 float
        return float(obj) if obj % 1 != 0 else int(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimal_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_to_native(item) for item in obj]
    return obj

def prepare_sku_data(items: List[Dict]) -> List[Dict]:
    """准备SKU数据"""
    records = []
    current_time = int(time.time() * 1000)

    for item in items:
        # 转换 Decimal 类型
        item = convert_decimal_to_native(item)
        record = {
            "日期": current_time,
            "产品链接": {"text": item['url'], "link": item['url']},
            "skuid": str(item['skuid']),
            "sku名称": item['sku_name'],
            "页面价": item['page_price'],
            "页面价是否变化": item.get('price_changed', '否'),
            "sku名称是否变化": item.get('name_changed', '否'),
        }
        records.append(record)

    return records


def prepare_goods_data(items: List[Dict]) -> List[Dict]:
    """准备商品数据"""
    records = []
    current_time = int(time.time() * 1000) - 86400000

    for item in items:
        # 转换 Decimal 类型
        item = convert_decimal_to_native(item)
        record = {
            '日期': current_time,
            "商品id": item['offerid'],
            "产品链接": {"text": item['url'], "link": item['url']},
            "上架时间": item['publish_time'],
            "商品类目": item['category'],
            "复购率": item['repurchase_rate'],
            "当天销售订单数": item['day_order_count'],
            "当天销售件数": item['day_sale_quantity'],
            "当天销售额": item['day_sales_volume'],
            "30天订单数": item['order_30d'],
            "30天销售额": item['sales_volume_30d'],
            "30天销售件数": item['sale_quantity_30d'],
            "sku数量": item['sku_count'],
            "总订单数": item['total_order_count'],
            "总件数": item['total_sale_count'],
            "店铺名称": item['shop_name'],
            "7天订单增长率": item['bookedCount7dGrowthRate'],
            "类目是否发生变化": item.get('category_changed', '否'),
            "sku数量是否发生变化": item.get('count_changed', '否')
        }
        records.append(record)

    return records


async def init_page_and_filter(page, logger):
    """
    初始化页面和筛选器
    刷新页面并重新创建筛选器
    """
    # 刷新页面
    await page.goto(
        "https://www.dianleida.net/1688/competeShop/category/library/",
        wait_until="networkidle"
    )

    await asyncio.sleep(1)

    # 关闭弹窗
    await close_popup_if_exists3(page, logger)
    await close_btn_if_exists(page, logger)
    await close_popup_if_exists(page, logger)

    # 重新创建筛选器
    filter_bot = OfferFilterAutomation(page, logger)

    return filter_bot



async def fetch_batch_data(
        page,
        logger,
        batch_ids: List[str],
        batch_num: int,
        total_batches: int,
        filter_bot: OfferFilterAutomation,
        include_sku: bool = True
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """获取单批数据"""
    batch_good_ids = '、'.join(batch_ids)

    logger.info(f"🔍 处理第 {batch_num}/{total_batches} 批，共 {len(batch_ids)} 个商品")
    logger.info(f"📋 商品ID: {batch_good_ids}")

    # 应用筛选条件
    await filter_bot.apply_all('商品ID', batch_good_ids)
    logger.info(f"✅ 第 {batch_num} 批筛选条件已设置")

    # 等待数据加载
    await asyncio.sleep(3)

    # 获取这一批的数据
    api_capture = OfferApiCapture(page, logger)

    if include_sku:
        # 包含SKU的完整获取 - 需要实现这个方法
        # 暂时使用 capture_without_sku
        batch_items, batch_sku_items, timeout_offers = await api_capture.capture_without_sku(batch_good_ids)
    else:
        # 只获取商品基本信息，不获取SKU
        batch_items, batch_sku_items, timeout_offers = await api_capture.capture_without_sku(batch_good_ids)

    logger.info(f"✅ 第 {batch_num} 批完成，获取到 {len(batch_items)} 个商品，{len(batch_sku_items)} 个SKU")

    return batch_items, batch_sku_items, timeout_offers


async def save_batch_without_sku(
        batch_items: List[Dict],
        storage: Ali1688MonitorStorage,
        logger,
        batch_num: int,
        total_batches: int
):
    """保存单批数据到数据库（只保存商品，不保存SKU）"""
    if not batch_items:
        return

    # 先获取所有商品的旧数据（批量获取，避免多次查询）
    url_to_old_data = {}
    for item in batch_items:
        url = item["url"]
        if url not in url_to_old_data:
            url_to_old_data[url] = storage.get_last_product(url)

    # 变化检测和标记
    for item in batch_items:
        try:
            url = item["url"]
            old_data = url_to_old_data.get(url)

            # 变化检测
            change = Ali1688ChangeDetector.detect_product(old_data, item)

            # 只有真正发生变化时才标记为"是"
            item["category_changed"] = "是" if change.get("category_changed") else "否"
            item["count_changed"] = "是" if change.get("sku_count_changed") else "否"

            # 调试日志
            if item["category_changed"] == "是":
                logger.info(
                    f"  📂 商品 {item['offerid']} 类目变化: {old_data.get('category') if old_data else '新'} -> {item['category']}")
            if item["count_changed"] == "是" and old_data:
                logger.info(
                    f"  🔢 商品 {item['offerid']} SKU数量变化: {old_data.get('sku_count', 0)} -> {item['sku_count']}")

        except Exception as e:
            logger.exception(f"商品变化检测失败: {e}")
            item["category_changed"] = "否"
            item["count_changed"] = "否"

    # 保存商品信息到数据库
    for item in batch_items:
        storage.save_product(item)

    logger.info(f"💾 第{batch_num}/{total_batches}批：已保存 {len(batch_items)} 个商品到数据库")



async def handle_timeout_offers(timeout_offers: List[Dict], logger):
    """处理超时商品"""
    if not timeout_offers:
        return

    logger.warning("=" * 60)
    logger.warning(f"⏰ 超时商品汇总（共 {len(timeout_offers)} 个）")
    logger.warning("=" * 60)

    for idx, offer in enumerate(timeout_offers, 1):
        logger.warning(f"{idx}. 商品ID: {offer['offer_id']}")
        logger.warning(f"   链接: {offer['url']}")
        logger.warning(f"   超时时间: {offer['time']}")
        logger.warning(f"   错误信息: {offer['error']}")
        logger.warning("-" * 40)

    # 发送到钉钉
    try:
        content = f"### ⏰ 1688商品请求超时提醒\n\n### 超时商品列表（共 {len(timeout_offers)} 个）\n\n"

        for idx, offer in enumerate(timeout_offers, 1):
            content += f"{idx}. 商品ID：{offer['offer_id']}\n"
            content += f"   链接：[点击查看]({offer['url']})\n"
            content += f"   超时时间：{offer['time']}\n"
            content += f"   错误信息：{offer['error']}\n\n"

        ding_bot_send('me', content)
        logger.info(f"✅ 已发送 {len(timeout_offers)} 个超时商品到钉钉")
    except Exception as e:
        logger.error(f"❌ 发送超时商品到钉钉失败: {e}")


async def send_changes_from_database(storage: Ali1688MonitorStorage, logger):
    """
    从数据库查询今天有变化的记录并发送通知
    """
    logger.info("🔍 从数据库查询今天的变化记录...")

    # 查询今天有变化的商品（类目变化或SKU数量变化）
    changed_products = storage.get_today_changed_products()

    # 查询今天有变化的SKU（价格变化或名称变化）
    changed_skus = storage.get_today_changed_skus()

    if not changed_products and not changed_skus:
        logger.info("✅ 今天没有变化数据")
        return [], []

    # 使用字典去重
    category_changed_map = {}  # url -> product
    count_changed_map = {}  # url -> product
    name_changed_map = {}  # (url, skuid) -> sku
    price_changed_map = {}  # (url, skuid) -> sku

    # 处理商品变化 - 去重
    for product in changed_products:
        url = product['url']
        if product.get('category_changed') == '是':
            if url not in category_changed_map:
                category_changed_map[url] = product
                logger.info(f"📂 检测到类目变化: {url}")

        if product.get('count_changed') == '是':
            if url not in count_changed_map:
                count_changed_map[url] = product
                logger.info(f"🔢 检测到SKU数量变化: {url}")

    # 处理SKU变化 - 去重
    for sku in changed_skus:
        url = sku['url']
        skuid = sku['skuid']
        key = (url, skuid)

        if sku.get('name_changed') == '是':
            if key not in name_changed_map:
                name_changed_map[key] = sku
                logger.info(f"📝 检测到SKU名称变化: {url} - {skuid}")

        if sku.get('price_changed') == '是':
            if key not in price_changed_map:
                price_changed_map[key] = sku
                logger.info(f"💰 检测到SKU价格变化: {url} - {skuid}")

    # 构建通知消息
    category_msg_list = []
    count_msg_list = []
    name_msg_list = []
    price_msg_list = []

    # 处理商品变化
    for url, product in category_changed_map.items():
        msg = f"""
商品: {url}
商品id: {product['offerid']}
类目已变化（从{product.get('old_category', '无')}变为{product['category']}）
"""
        category_msg_list.append(msg)

    for url, product in count_changed_map.items():
        old_count = product.get('old_sku_count', '?')
        new_count = product.get('sku_count', '?')
        # 只有当数量真正不同时才发送
        if str(old_count) != str(new_count):
            msg = f"""
商品: {url}
商品id: {product['offerid']}
SKU数量已变化（从{old_count}变为{new_count}）
"""
            count_msg_list.append(msg)
        else:
            logger.warning(f"⚠️ SKU数量变化但数值相同: {url}, old={old_count}, new={new_count}")

    # 处理SKU变化
    for (url, skuid), sku in name_changed_map.items():
        msg = f"""
商品: {url}
skuid: {skuid}
sku名称: {sku['sku_name']}
SKU名称已变化（从{sku.get('old_sku_name', '?')}变为{sku['sku_name']}）
"""
        name_msg_list.append(msg)

    for (url, skuid), sku in price_changed_map.items():
        msg = f"""
商品: {url}
skuid: {skuid}
sku名称: {sku['sku_name']}
价格已变化（从{sku.get('old_price', '?')}变为{sku['page_price']}）
"""
        price_msg_list.append(msg)

    # 发送钉钉通知
    if category_msg_list:
        ding_bot_send('me', '🚨1688商品类目变化\n' + "\n".join(category_msg_list))
        logger.info(f"📤 已发送 {len(category_msg_list)} 个类目变化通知")

    if count_msg_list:
        ding_bot_send('me', '🚨1688SKU数量变化\n' + "\n".join(count_msg_list))
        logger.info(f"📤 已发送 {len(count_msg_list)} 个SKU数量变化通知")

    if name_msg_list:
        ding_bot_send('me', '🚨1688SKU名称变化\n' + "\n".join(name_msg_list))
        logger.info(f"📤 已发送 {len(name_msg_list)} 个SKU名称变化通知")

    if price_msg_list:
        ding_bot_send('me', '🚨1688SKU价格变化\n' + "\n".join(price_msg_list))
        logger.info(f"📤 已发送 {len(price_msg_list)} 个价格变化通知")

    logger.info(
        f"📊 统计：类目变化{len(category_msg_list)}个，SKU数量变化{len(count_msg_list)}个，"
        f"SKU名称变化{len(name_msg_list)}个，价格变化{len(price_msg_list)}个")

    # 返回去重后的数据
    return list(category_changed_map.values()) + list(count_changed_map.values()), \
           list(name_changed_map.values()) + list(price_changed_map.values())


async def upload_today_data_to_dingtalk(storage: Ali1688MonitorStorage, logger, changed_products=None,
                                        changed_skus=None):
    """
    上传今天爬取的所有数据到钉钉文档
    """
    logger.info("📤 上传今天爬取的所有数据到钉钉文档...")

    # 如果没有传入变化数据，则从数据库获取
    if changed_products is None or changed_skus is None:
        changed_products = storage.get_today_changed_products()
        changed_skus = storage.get_today_changed_skus()

    # 获取今天爬取的所有商品数据（包括未变化的）
    all_today_products = storage.get_today_all_products()
    all_today_skus = storage.get_today_all_skus()

    if not all_today_products and not all_today_skus:
        logger.info("✅ 今天没有数据需要上传")
        return

    # 上传所有商品数据
    if all_today_products:
        records_goods = prepare_goods_data(all_today_products)
        if records_goods:
            upload_multiple_records(
                config=config,
                records=records_goods,
                logger=logger
            )
            logger.info(f"📤 已上传 {len(records_goods)} 个商品数据到钉钉")

    # 上传所有SKU数据
    if all_today_skus:
        records_sku = prepare_sku_data(all_today_skus)
        if records_sku:
            upload_multiple_records(
                config=config_sku,
                records=records_sku,
                logger=logger
            )
            logger.info(f"📤 已上传 {len(records_sku)} 个SKU数据到钉钉")


async def capture_without_sku(self, good_id):
    """只获取商品基本信息，不获取SKU"""
    self.logger.info(f'开始获取商品:{good_id}的基本信息')
    list_data = await self.capture_once(good_id)

    if not list_data:
        return [], []

    items = await self.parse(list_data)
    total_count = len(items)

    if total_count == 0:
        return [], []

    # 记录开始时间
    start_time = datetime.now()
    self.logger.info(f"🚀 开始捕获商品数据，共 {total_count} 个商品")

    # 总体统计
    total_elapsed = (datetime.now() - start_time).total_seconds()
    self.logger.info(f"✅ 商品基本信息捕获完成，总耗时: {total_elapsed:.2f}秒")
    self.logger.info(f"   - 商品数: {len(items)}")

    # 返回商品列表和空SKU列表
    return items, []


async def get_data_with_batch_processing():
    """分批获取数据并实时保存到数据库"""
    login = DianLeiDaLogin('ali1688_goods_monitor', headless=False)

    try:
        await login.login()
        page = login.page
        logger = login.logger


        # 获取所有商品ID
        all_good_ids = test_query_records()


        id_list = all_good_ids.split('、')[:] if all_good_ids else []  # 测试用5个

        if not id_list:
            logger.warning("⚠️ 没有找到商品ID")
            return None

        logger.info(f"📦 共找到 {len(id_list)} 个商品ID，将分批查询（每批10个）")

        # 分批处理：每20个商品一批
        batch_size = 20
        all_timeout_offers = []

        # 初始化页面和筛选器（第一次）
        filter_bot = await init_page_and_filter(page, logger)

        # 初始化存储
        storage = Ali1688MonitorStorage()

        # 计算总批次数
        total_batches = (len(id_list) + batch_size - 1) // batch_size

        # ========== 第一阶段：只查询商品基本信息，不查询SKU ==========
        logger.info("=" * 60)
        logger.info("📊 第一阶段：获取商品基本信息")
        logger.info("=" * 60)

        all_items = []  # 收集所有商品信息

        for i in range(0, len(id_list), batch_size):
            batch_ids = id_list[i:i + batch_size]
            batch_num = i // batch_size + 1

            # 如果不是第一批，刷新页面并重新初始化筛选器
            if batch_num > 1:
                logger.info(f"🔄 刷新页面，准备处理第 {batch_num} 批...")
                filter_bot = await init_page_and_filter(page, logger)
                await asyncio.sleep(2)

            # 获取当前批次数据（只获取商品基本信息，不获取SKU）
            batch_items, batch_sku_items, timeout_offers = await fetch_batch_data(
                page, logger, batch_ids, batch_num, total_batches, filter_bot, include_sku=False
            )

            # 打印这一批的商品信息
            logger.info(f"📋 第 {batch_num} 批商品信息:")
            for idx, item in enumerate(batch_items, 1):
                logger.info(f"  {idx}. 商品ID: {item['offerid']}, 店铺: {item['shop_name']}, SKU数: {item['sku_count']}")

            # 收集商品信息
            all_items.extend(batch_items)

            # 收集超时商品
            if timeout_offers:
                all_timeout_offers.extend(timeout_offers)

            # 保存商品基本信息到数据库
            await save_batch_without_sku(
                batch_items, storage, logger, batch_num, total_batches
            )

            # 随机暂停
            if i + batch_size < len(id_list):
                pause_time = random.uniform(3, 7)
                logger.info(f"⏸️  随机暂停 {pause_time:.1f} 秒...")
                await asyncio.sleep(pause_time)

        # 处理超时商品
        await handle_timeout_offers(all_timeout_offers, logger)

        logger.info(f"🎉 第一阶段完成，共获取 {len(all_items)} 个商品基本信息")

        # ========== 第二阶段：单独获取SKU信息 ==========
        # logger.info("=" * 60)
        # logger.info("📊 第二阶段：获取SKU信息")
        # logger.info("=" * 60)
        #
        # if all_items:
        #     logger.info(f"🔍 准备获取 {len(all_items)} 个商品的SKU信息...")
        #
        #     # 创建URL列表
        #     url_list = [item["url"] for item in all_items]
        #
        #     # 打印即将获取SKU的商品列表
        #     logger.info("📋 即将获取SKU的商品列表:")
        #     for idx, item in enumerate(all_items, 1):
        #         logger.info(f"  {idx}. {item['url']} (SKU数: {item['sku_count']})")
        #
        #     # 创建一个新的 OfferApiCapture 实例
        #     temp_api = OfferApiCapture(page, logger)
        #
        #     try:
        #         # 调用SKU获取方法
        #         all_sku_items = await temp_api.capture_sku_only(url_list)
        #
        #         # 打印SKU信息
        #         logger.info(f"📋 获取到的SKU信息 (共 {len(all_sku_items)} 条):")
        #         # for idx, sku in enumerate(all_sku_items, 1):
        #         #     logger.info(f"  {idx}. SKUID: {sku['skuid']}, 名称: {sku['sku_name']}, 价格: {sku['page_price']}")
        #
        #         # ========== 在保存SKU前进行变化检测 ==========
        #         logger.info("=" * 60)
        #         logger.info("🔍 检测SKU变化...")
        #         logger.info("=" * 60)
        #
        #         # 批量获取所有SKU的历史数据
        #         url_skus_map = {}
        #         for sku_item in all_sku_items:
        #             url = sku_item["url"]
        #             if url not in url_skus_map:
        #                 url_skus_map[url] = storage.get_last_sku(url)
        #
        #         # 用于去重的集合
        #         changed_price_skus = set()  # 存储 (url, skuid) 组合
        #         changed_name_skus = set()
        #
        #         for sku_item in all_sku_items:
        #             try:
        #                 url = sku_item["url"]
        #                 old_skus = url_skus_map.get(url, [])
        #                 old_map = {i["skuid"]: i for i in old_skus}
        #                 old_data = old_map.get(sku_item["skuid"])
        #
        #                 # 检测价格变化
        #                 price_changed = False
        #                 name_changed = False
        #
        #                 if old_data:
        #                     # ========== 修复价格比较逻辑 ==========
        #                     old_price_raw = old_data.get("page_price")
        #                     new_price_raw = sku_item["page_price"]
        #
        #                     # 方法1：使用Decimal精确比较（推荐）
        #                     from decimal import Decimal, ROUND_HALF_UP
        #
        #                     def normalize_price(price):
        #                         if price is None:
        #                             return None
        #                         try:
        #                             # 转换为字符串再转Decimal，避免精度问题
        #                             if isinstance(price, Decimal):
        #                                 price_str = str(price)
        #                             else:
        #                                 price_str = str(price)
        #                             dec = Decimal(price_str)
        #                             # 量化为两位小数
        #                             return dec.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
        #                         except:
        #                             return None
        #
        #                     old_price_dec = normalize_price(old_price_raw)
        #                     new_price_dec = normalize_price(new_price_raw)
        #
        #                     if old_price_dec != new_price_dec:
        #                         price_changed = True
        #                         key = (url, sku_item["skuid"])
        #                         if key not in changed_price_skus:
        #                             changed_price_skus.add(key)
        #                             logger.info(
        #                                 f"  💰 SKU {sku_item['skuid']} 价格变化: {old_price_dec} -> {new_price_dec}")
        #                     else:
        #                         # 价格相同，记录调试信息
        #                         logger.debug(f"  ✓ SKU {sku_item['skuid']} 价格相同: {new_price_dec}")
        #
        #                     # 名称变化检测（保持原逻辑）
        #                     old_name = old_data.get("sku_name")
        #                     new_name = sku_item["sku_name"]
        #                     if old_name != new_name:
        #                         name_changed = True
        #                         key = (url, sku_item["skuid"])
        #                         if key not in changed_name_skus:
        #                             changed_name_skus.add(key)
        #                             logger.info(f"  📝 SKU {sku_item['skuid']} 名称变化: {old_name} -> {new_name}")
        #
        #                     # 标记变化
        #                     sku_item["price_changed"] = "是" if price_changed else "否"
        #                     sku_item["name_changed"] = "是" if name_changed else "否"
        #                 else:
        #                     # 新SKU，标记为未变化
        #                     sku_item["price_changed"] = "否"
        #                     sku_item["name_changed"] = "否"
        #
        #             except Exception as e:
        #                 logger.exception(f"SKU变化检测失败: {e}")
        #                 sku_item["price_changed"] = "否"
        #                 sku_item["name_changed"] = "否"
        #
        #         # 保存SKU到数据库
        #         logger.info("💾 保存SKU数据到数据库...")
        #         for sku_item in all_sku_items:
        #             storage.save_sku(sku_item)
        #
        #         logger.info(f"🎉 第二阶段完成，共获取并保存 {len(all_sku_items)} 个SKU信息")
        #
        #     except Exception as e:
        #         logger.error(f"❌ SKU获取失败: {e}")
        #         import traceback
        #         traceback.print_exc()
        # else:
        #     logger.warning("⚠️ 没有商品信息，跳过SKU获取")
        #
        # logger.info(f"📊 总计：{len(all_items)} 个商品，{len(all_sku_items) if 'all_sku_items' in dir() else 0} 个SKU")

        return storage

    except Exception as e:
        logger.exception(f"获取数据失败: {e}")
        return None
    finally:
        await login.close()



async def main():
    total_start = time.perf_counter()
    logger = get_logger('ali1688_goods_monitor')

    logger.info('=' * 60)
    logger.info('🚀 开始1688竞品监控任务')
    logger.info('=' * 60)

    # 获取数据并保存到数据库（分两阶段：先商品后SKU）
    storage = await get_data_with_batch_processing()

    if not storage:
        logger.warning("⚠️ 未获取到任何数据")
        return

    # 第二步：从数据库查询今天有变化的记录
    changed_products, changed_skus = await send_changes_from_database(storage, logger)

    # 第三步：上传今天爬取的所有数据到钉钉文档
    await upload_today_data_to_dingtalk(storage, logger, changed_products, changed_skus)

    # 任务结束统计
    total_cost = time.perf_counter() - total_start

    # 统计变化数量
    product_count = len(changed_products) if changed_products else 0
    sku_count = len(changed_skus) if changed_skus else 0

    # 统计总数据量
    all_products = storage.get_today_all_products() if hasattr(storage, 'get_today_all_products') else []
    all_skus = storage.get_today_all_skus() if hasattr(storage, 'get_today_all_skus') else []

    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
    logger.info(f"📊 统计：今天共爬取 {len(all_products)} 个商品，{len(all_skus)} 个SKU")
    logger.info(f"📊 变化统计：{product_count} 个商品发生变化，{sku_count} 个SKU发生变化")

    ding_bot_send('me',
                  f'1688竞品监控任务完成\n总耗时：{format_seconds(total_cost)}\n'
                  f'今日爬取：{len(all_products)}个商品，{len(all_skus)}个SKU\n'
                  f'变化统计：{product_count}个商品，{sku_count}个SKU')
    logger.info('=' * 60)
    logger.info('✅ 数据处理完成')
    logger.info('=' * 60)


if __name__ == '__main__':
    asyncio.run(main())

