import pymysql
import requests
import json
from datetime import datetime
from utils.logger import get_logger
from utils.dingtalk_work_notifier import DingTalkWorkNotifier

logger = get_logger("Temu-Violation-Notify")

# ======================
# 1. 查询今天新增且金额 >= 500 的违规
# ======================
def fetch_today_new_high_violation(conn):
    sql = """
    SELECT 
        t.店铺,
        t.违规编号,
        t.违规类型,
        t.违规金额,
        t.违规发起时间
    FROM temu_violation_record t
    LEFT JOIN temu_violation_record y
      ON t.店铺 = y.店铺
     AND t.违规编号 = y.违规编号
     AND DATE(y.数据抓取时间) = CURDATE() - INTERVAL 1 DAY
    WHERE DATE(t.数据抓取时间) = CURDATE()
      AND y.id IS NULL
      AND t.违规金额 >= 500;
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(sql)
        return cursor.fetchall()



# ======================
# 2. 发送钉钉消息
# ======================
def send_dingtalk(records):

    APP_KEY = "dings13rpbdzmpis6dyo"
    APP_SECRET = "J0gloUMrhko4ca_Esar9jiVtef8vT-Qd5AJq9B3zYeyR7pEdoIHibaUFU8NZfz9o"
    AGENT_ID = 4036386083
    USER_IDS = ["106246005536840537"]  # 钉钉 userid

    notifier = DingTalkWorkNotifier(
        app_key=APP_KEY,
        app_secret=APP_SECRET,
        agent_id=AGENT_ID
    )

    notifier.send_text(
        user_ids=USER_IDS,
        title="库存异常",
        text="SKU123 当前库存为 0，请立即处理"
    )


# ======================
# 3. 主流程
# ======================
def main():
    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="1234",
        database="py_spider",
        charset="utf8mb4"
    )

    try:
        records = fetch_today_new_high_violation(conn)

        if records:
            logger.warning(f"发现 {len(records)} 条新增高额违规，开始通知")
            send_dingtalk(records)

        else:
            logger.info("今日无新增高额违规")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
