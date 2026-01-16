from pathlib import Path
import asyncio
from utils.logger import get_logger
from typing import List, Dict, Any
from utils.dingding_doc import upload_multiple_records
from services.search.xiaozhuxiong import XiaozhuxiongSearch
from services.messaging.xiaozhuxiong_chat import SupplierChatService
from services.search.xiaoniaoyun_playwright import ToysAASBot
from services.search.yicai import YiCaiSearch


import os

config = {
        "base_id": "KGZLxjv9VG03dPLZt4B3yZgjJ6EDybno",
        "sheet_id": "s4EvVZf", #以图搜图·厂商线索池
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }


logger=get_logger('search_factory')

# ================== 路径配置 ==================
BASE_DIR = Path(__file__).resolve().parent
IMG_DIR = BASE_DIR / 'data' / 'img'
IMG_DIR.mkdir(parents=True, exist_ok=True)


# ================== 小竹熊（同步） ==================
def run_xiaozhuxiong(image_path: str, message: str):
    """
    小竹熊：图片搜索 + 建群（同步）
    """
    search = XiaozhuxiongSearch()
    chat = SupplierChatService()

    items,upload_img_url = search.search_by_image(image_path)

    sent_companies = set()  # 用来记录已经处理过的公司

    for item in items:
        company_number = item['companyNumber']


        if company_number in sent_companies:
            continue  # 已经发送过，跳过
        logger.info('正在给--item["供应商"]发送信息')
        target_id = chat.create_group(company_number)
        chat.send_text(target_id, upload_img_url, 'RC:ImgMsg')
        chat.send_text(target_id, message, 'RC:TxtMsg')

        sent_companies.add(company_number)  # 标记为已发送

    return items


# ================== 宜采（异步） ==================
async def run_yicai(image_path: str):
    """
    宜采：图片搜索（异步）
    """
    yicai = YiCaiSearch()
    items = await yicai.fetch(image_path)
    logger.info(f"✅ 宜采抓取完成：{len(items)} 条")
    return items


# ================== 宵鸟云（异步，批量） ==================
async def run_xiaoniaoyun(image_list: list[str], message: str):
    """
    宵鸟云：批量图片搜索 + 发送消息
    """
    bot = ToysAASBot(headless=False)

    success = await bot.init_browser() and await bot.login()
    if not success:
        logger.info("❌ 宵鸟云登录失败")
        return []

    items = await bot.process_images(
        image_list=image_list,
        message=message
    )

    await bot.close()
    return items



def prepare_upload_records(items):
    """
    将爬虫 items 转换为钉钉多维表可上传 records
    """
    records = []
    for item in items:
        record = {
            "平台": item.get("平台", ""),
            "搜图图片": {"text": item["搜图图片"], "link": item["搜图图片"]},
            "商品名称": item.get("商品名称", ""),
            "商品图片链接": {"text": item["商品图片链接"], "link": item["商品图片链接"]},
            "价格": float(item.get("价格", 0)) if item.get("价格") else 0,
            "供应商": item.get("供应商", ""),
            "联系人": item.get("联系人", ""),
            "手机号": item.get("手机号", ""),
            "QQ": item.get("QQ", ""),
            "地址": item.get("地址", ""),
            "爬取数据时间": item.get("爬取数据时间")
        }
        records.append(record)
    return records



# ================== 主流程 ==================
async def main():
    img_list = [
        str(IMG_DIR / "car.jpg"),
        # str(IMG_DIR / "basketball.png"),
    ]

    message = "这是你们公司生产的产品？有外贸证书？"

    # ---------- 单图平台 ----------
    for img in img_list:
        logger.info(f"\n📸 处理图片：{img}")

        # 小竹熊（同步）
        xzx_items = run_xiaozhuxiong(img, message)
        records = prepare_upload_records(xzx_items)
        upload_multiple_records(config,records)


        # 宜采（异步）
        yicai_items = await run_yicai(img)
        records = prepare_upload_records(yicai_items)
        upload_multiple_records(config, records)

    # ---------- 批量平台 ----------
    xny_items = await run_xiaoniaoyun(img_list, message)
    records = prepare_upload_records(xny_items)
    upload_multiple_records(config, records)


# ================== 程序入口 ==================
if __name__ == '__main__':
    asyncio.run(main())
