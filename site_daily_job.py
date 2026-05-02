import asyncio
from services.site.site_skc_sku_violate import SkcFetcher
from services.site.sku_site_status import SkuSiteStatusFetcher
from services.site.sku_site_exception import SkuException
from utils.logger import get_logger
from utils.dingding_doc import DingTalkTokenManager, upload_multiple_records, test_delete_records
from utils.dingtalk_bot import ding_bot_send

from datetime import datetime,timedelta
from pathlib import Path
import pandas as pd
import time
import re
from typing import Dict, List, Tuple, Any

import sys
import os

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from services.site.site_skc_sku_violate import SkcFetcher

SITE_DIR = Path(__file__).resolve().parent / "data" / "site"
SITE_DIR.mkdir(parents=True, exist_ok=True)

"""temu站点状态"""
def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"


async def fetch_shop_data(shop_name: str, logger) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[Dict]]:
    """
    抓取单个店铺的数据

    Args:
        shop_name: 店铺名称
        logger: 日志记录器

    Returns:
        tuple: (违规数据DataFrame, 状态数据DataFrame, 异常数据DataFrame, goods_list)
    """
    skc_fetcher = SkcFetcher(shop_name,'temu_site')
    status_fetcher = SkuSiteStatusFetcher(shop_name,'temu_site')
    site_exception = SkuException(shop_name,'temu_site')

    # ========= 1️⃣ 违规数据 =========
    logger.info(f'正在爬取店铺---{shop_name}---skc_sku_violate数据')
    all_data = await skc_fetcher.fetch()

    violate_rows = all_data["violate_rows"]
    goods_list = all_data["goodsIdSkuIdPairList"]

    # 如果没有违规数据，返回空数据
    if not violate_rows:
        logger.info(f'店铺---{shop_name}---没有违规数据')
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []

    df_violate = pd.DataFrame(violate_rows)

    # ========= 2️⃣ 站点状态数据 =========
    logger.info(f'正在爬取店铺---{shop_name}---sku_site_status数据')
    status_rows = []

    skc_seen = set()
    for item in violate_rows:
        skc_id = item["SKC"]
        if skc_id in skc_seen:
            continue
        skc_seen.add(skc_id)

        rows = await status_fetcher.fetch(skc_id)
        status_rows.extend(rows)

    df_status = pd.DataFrame(status_rows)

    # ========= 3️⃣ 异常数据 =========
    df_exception = pd.DataFrame()
    if goods_list:
        logger.info(f'正在爬取店铺---{shop_name}---sku_site_exception数据')
        exception_rows = await site_exception.fetch(goods_list)
        df_exception = pd.DataFrame(exception_rows)
    else:
        logger.info(f'店铺---{shop_name}---goods_list为空，跳过异常数据爬取')

    return df_violate, df_status, df_exception, goods_list


def process_shop_data(
        shop_name: str,
        df_violate: pd.DataFrame,
        df_status: pd.DataFrame,
        df_exception: pd.DataFrame,
        goods_list: List[Dict]
):
    """
    处理单个店铺的数据

    Args:
        shop_name: 店铺名称
        df_violate: 违规数据
        df_status: 状态数据
        df_exception: 异常数据
        goods_list: 商品列表

    Returns:
        pd.DataFrame: 处理后的最终数据
    """
    # ========= 1️⃣ 数据标准化 =========
    df_status[['SKU', '站点', '加站状态']] = df_status[['SKU', '站点', '加站状态']]

    # ========= 2️⃣ 异常数据处理 =========
    if not df_exception.empty and not df_violate.empty:
        # 用 violation 给 exception 补 SKU
        df_exception = df_exception.merge(
            df_violate[['skuId', 'SKU']].drop_duplicates(),
            how='left',
            on='skuId'
        )
    elif df_exception.empty:
        # 创建一个空的异常数据框
        df_exception = pd.DataFrame(columns=['SKU', '站点', '异常原因', 'skuId'])

    # ========= 3️⃣ 合并三张表 =========
    # 首先合并状态和违规数据
    if not df_status.empty and not df_violate.empty:
        df = df_status.merge(
            df_violate[['SKC', 'SKU', '站点', '违规原因']],
            how='left',
            on=['SKU', '站点']
        )
    else:
        df = pd.DataFrame()

    # 如果有异常数据，再合并异常数据
    if not df.empty and not df_exception.empty:
        df = df.merge(
            df_exception[['SKU', '站点', '异常原因']],
            how='left',
            on=['SKU', '站点']
        )
    elif not df.empty:
        # 如果没有异常数据，添加空列
        df['异常原因'] = pd.NA

    # ========= 4️⃣ 补全 SKC =========
    if not df.empty and not df_violate.empty:
        df = df.merge(
            df_violate[['SKU', 'SKC']].drop_duplicates(),
            how='left',
            on='SKU',
            suffixes=('', '_violate')
        )
        df['SKC'] = df['SKC'].fillna(df['SKC_violate'])
        if 'SKC_violate' in df.columns:
            df.drop(columns=['SKC_violate'], inplace=True)

    # ========= 5️⃣ 合并规则处理 =========
    if not df.empty:
        df[['违规原因', '异常原因']] = df[['违规原因', '异常原因']].replace('', pd.NA)

        # 获取当前处理时间戳（在分组前）
        process_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        final_df = (
            df
            .groupby(
                ['SKC', 'SKU', '加站状态', '违规原因', '异常原因'],
                dropna=False
            )['站点']
            .apply(lambda x: ','.join(sorted(set(x))))
            .reset_index()
        )

        final_df[['违规原因', '异常原因']] = final_df[['违规原因', '异常原因']].fillna('')

        # ========= 新增处理时间列 =========
        final_df.insert(0, '抓取时间', process_time)  # 插入到第一列

        # ========= 新增店铺列 =========
        final_df.insert(1, '店铺', shop_name)  # 插入到第二列（在时间列后面）

        # 列顺序（可读性更好）
        output_file = SITE_DIR / f"{shop_name}_站点加站最终结果_{datetime.now():%Y%m%d}.xlsx"
        final_df.to_excel(output_file, index=False)

        return final_df  # 👈 添加返回值
    else:
        return pd.DataFrame()  # 👈 返回空DataFrame


def prepare_upload_records(date) -> List[Dict]:
    """
    准备上传数据

    Args:
        date: 日期字符串

    Returns:
        List[Dict]: 上传记录列表
    """
    records = []

    SITE_DIR = Path(__file__).resolve().parent / "data" / "site"
    pattern = f"*{date}.xlsx"
    files = list(SITE_DIR.glob(pattern))

    if not files:
        return records

    # 读取Excel时强制所有列为字符串，并禁用NaN值
    df_list = [pd.read_excel(f, dtype=str, keep_default_na=False) for f in files]
    df = pd.concat(df_list, ignore_index=True)

    for _, row in df.iterrows():
        sites_str = ','.join([s.strip() for s in str(row.get('站点', '')).split(',') if s.strip()])

        record = {
            "店铺": str(row.get('店铺', '')),
            "SKC": str(row.get('SKC', '')),
            "货号": str(row.get('SKU', '')),
            "违规原因": [str(row['违规原因'])] if row.get('违规原因') else [],
            "站点": sites_str,
            "异常原因": str(row.get('异常原因', '')),
            "加站状态": str(row.get('加站状态', '')),
            "数据抓取日期": (row.get('抓取时间', ''))
        }
        records.append(record)

    return records



async def main():
    total_start = time.perf_counter()

    logger = get_logger('temu_site')

    config_yestody = {"base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
                      "sheet_id": "temu站点状态-昨天",
                      "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"}
    logger.info('开始清除昨天报表的数据')
    # 先清除数据
    test_delete_records(logger=logger, config=config_yestody)

    config = { "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
               "sheet_id":"temu站点状态-当天" ,
               "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE" }

    logger.info('开始清除今天报表的数据')

    # 先清除数据
    test_delete_records(logger=logger, config=config)

    shop_name_list = [
        "2106-Temu全托管", "2103-Temu全托管", "2102-Temu全托管", "2101-Temu全托管KA",
        "112-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管", "1105-Temu全托管", "1104-Temu全托管",
        "1103-Temu全托管", "1102-Temu全托管", "1101-Temu全托管",
        "110-Temu全托管KA", "109-Temu全托管KA", "108-Temu全托管", "106-Temu全托管", "105-Temu全托管",
        "104-Temu全托管", "103-Temu全托管", "102-Temu全托管", "101-Temu全托管",
    ]
    # shop_name_list=["103-Temu全托管"]



    for shop_name in shop_name_list:
        try:
            # ========= 1️⃣ 抓取数据 =========
            df_violate, df_status, df_exception, goods_list = await fetch_shop_data(shop_name, logger)

            # 如果没有违规数据，跳过这个店铺
            if df_violate.empty:
                logger.info(f'店铺---{shop_name}---没有违规数据，跳过')
                continue

            # ========= 2️⃣ 处理数据 =========
            final_df = process_shop_data(shop_name, df_violate, df_status, df_exception, goods_list)

            if final_df.empty:
                logger.info(f'店铺---{shop_name}---处理后数据为空，跳过')
                continue
        except Exception as e:
            logger.error(f'处理店铺 {shop_name} 时出错: {str(e)}')
            continue

    # ========= 3️⃣ 准备上传数据 =========
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    date = datetime.now().strftime("%Y%m%d")

    yesterday_records = prepare_upload_records(yesterday)
    upload_multiple_records(logger=logger, config=config_yestody, records=yesterday_records)

    records = prepare_upload_records(date)
    upload_multiple_records(logger=logger, config=config, records=records)

    total_cost = time.perf_counter() - total_start
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
    ding_bot_send('me',f'temu_site_daily_job任务结束，总耗时：{format_seconds(total_cost)}')


if __name__ == "__main__":
    asyncio.run(main())