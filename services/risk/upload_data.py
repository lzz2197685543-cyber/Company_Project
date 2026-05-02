# -*- coding: utf-8 -*-
"""
Temu 数据 → 钉钉多维表
包含：
1. 今日违规记录上传
2. 今日限制金额上传
"""

import pymysql
from datetime import datetime
from decimal import Decimal
from utils.dingtalk_bot import ding_bot_send

from utils.dingding_doc import (
    DingTalkTokenManager,
    DingTalkSheetUploader,
    DingTalkSheetDeleter,upload_multiple_records
)
from utils.logger import get_logger


logger = get_logger("temu_risk_daily_job")


# ======================
# 通用格式化工具
# ======================

def format_datetime(dt):
    """DATETIME → str"""
    if dt is None:
        return ""
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


def format_decimal(val):
    """Decimal → float（解决 JSON 序列化问题）"""
    if val is None:
        return 0
    if isinstance(val, Decimal):
        return float(val)
    return val



# ======================
# 钉钉删除通用方法
# ======================

def test_delete_records(config):
    """测试删除功能"""

    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建删除器
    deleter = DingTalkSheetDeleter(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )
    # 删除所有记录（谨慎使用！）
    # 注意：这里使用confirm=False，不会实际执行删除
    delete_all_result = deleter.delete_all_records(
        batch_size=50,
        delay=0.2,
        confirm=True  # 设置为True才会实际删除
    )
    print(f"删除所有记录结果: {delete_all_result.get('message')}")

    return deleter


# ======================
# 违规记录（temu_violation_record）
# ======================

def select_today_violation_records(conn):
    """
    查询【今日】违规记录
    """
    sql = """
    SELECT
        店铺,
        违规编号,
        违规类型,
        违规颗粒,
        备货单号,
        sku,
        进度,
        违规金额,
        违规发起时间,
        数据抓取时间
    FROM temu_violation_record
    WHERE 数据抓取时间 >= CURDATE()
      AND 数据抓取时间 < CURDATE() + INTERVAL 1 DAY
    ORDER BY 数据抓取时间 DESC
    """

    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()

        records = []
        for r in rows:
            records.append({
                "数据抓取时间": format_datetime(r["数据抓取时间"]),
                "店铺": r["店铺"],
                "违规编号": r["违规编号"],
                "违规类型": r["违规类型"],
                "违规颗粒": r["违规颗粒"],
                "备货单号": r["备货单号"],
                "SKC": r["sku"],
                "进度": r["进度"],
                "违规金额": format_decimal(r["违规金额"]),
                "违规发起时间": format_datetime(r["违规发起时间"]),
            })

        logger.info(f"违规记录查询成功：{len(records)} 条")
        return records

    except Exception:
        logger.exception("查询违规记录失败")
        return []


# ======================
# 限制金额（temu_funds_restriction）
# ======================

def select_funds_restriction_by_day(conn, day_offset: int):
    """
    查询指定日期的数据抓取时间的限制金额记录
    :param day_offset: 0=今天，1=昨天
    """
    sql = """
    SELECT
        店铺,
        冻结类型,
        限制原因,
        限制总金额,
        限制时间,
        数据抓取时间
    FROM temu_funds_restriction
    WHERE 数据抓取时间 >= CURDATE() - INTERVAL %s DAY
      AND 数据抓取时间 <  CURDATE() - INTERVAL %s DAY + INTERVAL 1 DAY
    ORDER BY 数据抓取时间 DESC
    """

    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, (day_offset, day_offset))
            rows = cursor.fetchall()

        records = []
        for row in rows:
            records.append({
                "店铺": row.get("店铺"),
                "限制原因": row.get("限制原因"),
                "限制总金额(CNY)": format_decimal(row.get("限制总金额")),
                "限制时间": format_datetime(row.get("限制时间")),
                "数据抓取时间": format_datetime(row.get("数据抓取时间")),
            })

        logger.info(f"查询限制金额成功（day_offset={day_offset}），共 {len(records)} 条")
        return records

    except Exception:
        logger.exception("查询限制金额失败")
        return []


def select_today_funds_restriction_records(conn):
    return select_funds_restriction_by_day(conn, day_offset=0)

def select_yesterday_funds_restriction_records(conn):
    return select_funds_restriction_by_day(conn, day_offset=1)



# ======================
# 主流程
# ======================

def main():
    conn = pymysql.connect(
        host='rm-bp186omby3lautfn0no.mysql.rds.aliyuncs.com',
        port=3306,
        user='root_lxz',
        password='Lxz123456',
        database='py_spider',
        charset="utf8mb4"
    )

    try:
        # ========= 违规记录 =========
        violation_records = select_today_violation_records(conn)
        logger.info(f"今日违法记录 {len(violation_records)} 条")
        upload_multiple_records(
            config={
                "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
                "sheet_id": "temu违规金额",
                "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
            },
            records=violation_records,
            logger=logger,
        )

        # ========= 限制金额 =========
        # 先删掉限制金额
        test_delete_records(config={
                "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
                "sheet_id": "temu资金限制-当天",
                "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
            })

        test_delete_records(config={
            "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
            "sheet_id": "temu资金限制-昨天",
            "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
        })

        # 今日限制金额
        funds_today_records = select_today_funds_restriction_records(conn)
        logger.info(f"今日限制金额 {len(funds_today_records)} 条")

        upload_multiple_records(
            config={
                "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
                "sheet_id": "temu资金限制-当天",
                "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
            },
            records=funds_today_records,
            logger=logger,

        )

        # 昨日限制金额
        funds_yesterday_records = select_yesterday_funds_restriction_records(conn)
        logger.info(f"昨日限制金额 {len(funds_yesterday_records)} 条")

        upload_multiple_records(
            config={
                "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
                "sheet_id": "temu资金限制-昨天",
                "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
            },
            records=funds_yesterday_records,
            logger=logger
        )


    finally:
        conn.close()


def run_upload_data():
    main()

# run_upload_data()
