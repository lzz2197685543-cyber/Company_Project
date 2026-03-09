import asyncio
import time
from datetime import datetime
from services.amazon_goods_monitor.amazon_product_crawler import AmazonProductCrawler
from storage.amazon_monitor_storage import AmazonMonitorStorage
from monitor.amazon_change_detector import ChangeDetector
from utils.dingtalk_bot import ding_bot_send
from utils.dingding_doc import upload_multiple_records,DingTalkTokenManager,DingTalkSheetQuery
from utils.logger import get_logger
import json

logger = get_logger("amazon_goods_monitor")


config = {
    "base_id": "gvNG4YZ7JnwebGnESqKO3r6182LD0oRE",
    "sheet_id": 'hERWDMS',
    "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
}

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"

def test_query_records():
    config = {
        "base_id": "gvNG4YZ7JnwebGnESqKO3r6182LD0oRE",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE",
        "sheet_id": "RZdYSYw"
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

        product_name = fields.get("产品名")

        link_info = fields.get("产品链接", {})
        url = link_info.get("link")

        if url and "amazon" in url:
            item = {
                "产品名": product_name,
                "产品链接": url
            }
            items.append(item)

    return items


asin_url_list = test_query_records()

print(asin_url_list)

def prepare_table_data(items):
    """构造第一个钉钉表数据（揽收丢件表）"""
    records = []
    for item in items:

        # 直接构建fields，不要嵌套两层
        record = {
            "产品名": item['product_name'],
            "产品链接": {"text": item['url'], "link": item['url']},
            "日期": int(time.time()*1000),
            "页面价": item.get("price", ""),
            "优惠券": item.get("coupon", ""),
            "销量": item.get("sales", ""),
            "大类排名": item.get("bsr_rank", ""),
            "小类排名": item.get('sub_rank', ''),
            "全部流量词": item.get('all_keywords', 0),
            "自然搜索词": item.get('natural_keywords', 0),
            "广告流量词": item.get('ads_keywords', 0),
            "搜索推荐词": item.get('recommend_keywords', 0),
            "评分": item.get('rating', ''),
            "评分数": item.get('reviews', ''),
            "标题": item.get('title', ''),
            "标题是否发生变化": item.get('title_changed', ''),
            "图片链接": {"text": item['img_url'], "link": item['img_url']},
        }

        records.append(record)

    return records


async def main():
    total_start = time.perf_counter()

    crawler = AmazonProductCrawler("amazon_goods_monitor")

    product_list = await crawler.crawl_batch(asin_url_list)

    storage = AmazonMonitorStorage("amazon_goods_monitor")

    message_list = []

    message_list1=[]

    for product in product_list:

        asin = product["asin"]

        old_data = storage.get_yesterday_data(asin)

        change = ChangeDetector.detect(old_data, product)

        # ---------------- 价格变化 ----------------
        if change["price_changed"]:

            msg = f"""
商品: {product['product_name']}

昨日价格: {change['old_price']}
今日价格: {product['price']}

链接: {product['url']}
"""

            message_list.append(msg)

        # ---------------- 标题变化 ----------------
        if change["title_changed"]:
            product["title_changed"] = "是"

            msg1 = f"""
商品: {product['product_name']}

昨日标题: {change['old_title']} 
今日标题: {product['title']} 
链接:{product['url']}
"""
            message_list1.append(msg1)

        # 保存数据库
        storage.save(product)

    # ---------------- 汇总发送 ----------------

    if message_list:

        final_msg = "🚨 Amazon商品价格变化提醒\n"

        final_msg += "\n".join(message_list)

        ding_bot_send('me',final_msg)
        print(final_msg)

    if message_list1:
        final_msg = "🚨 Amazon商品标题变化\n"

        final_msg += "\n".join(message_list1)

        ding_bot_send('me', final_msg)
        print(final_msg)

    else:
        logger.info("今日无价格变化")

    # ---------------- 上传钉钉表 ----------------

    records = prepare_table_data(product_list)

    upload_multiple_records(
        config=config,
        records=records,
        logger=logger
    )

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
    ding_bot_send('me', f'亚马逊竞品监控任务完成,总耗时：{format_seconds(total_cost)}')


if __name__ == "__main__":
    asyncio.run(main())
