import pymysql
from utils.logger import get_logger
from utils.config_loader import get_dingtalk_config
from utils.dingtalk_work_notifier import DingTalkWorkNotifier

logger = get_logger("Temu-Daily-Notify")


# ======================
# 通用：钉钉通知人
# ======================
def get_notifier_and_users():
    ding_cfg = get_dingtalk_config()
    user_ids = [
        ding_cfg["userid"]["邹雯雯"],
        ding_cfg["userid"]["赵结贤"],
        ding_cfg["userid"]["李惠清"],
        ding_cfg["userid"]["it"],
    ]
    return DingTalkWorkNotifier(), user_ids


# ======================
# ① 今天新增 ≥500 的违规
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


def notify_violation(records):
    notifier, user_ids = get_notifier_and_users()

    for r in records:
        text = (
            f"【新增违规告警】\n"
            f"店铺：{r['店铺']}\n"
            f"违规编号：{r['违规编号']}\n"
            f"违规类型：{r['违规类型']}\n"
            f"违规金额：{r['违规金额']}"
        )
        logger.warning(text)

        # notifier.send_text(
        #     user_ids=user_ids,
        #     title="Temu 新增违规",
        #     text=text
        # )


# ======================
# ② 资金限制金额差值 ≥500
# ======================
def fetch_today_diff_high_restriction(conn):
    sql = """
    WITH daily_sum AS (
        SELECT
            店铺,
            限制原因,
            DATE(数据抓取时间) AS stat_date,
            SUM(限制总金额) AS total_amount
        FROM temu_funds_restriction
        WHERE DATE(数据抓取时间) IN (
            CURDATE(),
            CURDATE() - INTERVAL 1 DAY
        )
        GROUP BY 店铺, 限制原因, DATE(数据抓取时间)
    )
    SELECT
        t.店铺,
        t.限制原因,
        IFNULL(y.total_amount, 0) AS yesterday_amount,
        t.total_amount AS today_amount,
        (t.total_amount - IFNULL(y.total_amount, 0)) AS diff_amount
    FROM daily_sum t
    LEFT JOIN daily_sum y
           ON t.店铺 = y.店铺
          AND t.限制原因 = y.限制原因
          AND y.stat_date = CURDATE() - INTERVAL 1 DAY
    WHERE t.stat_date = CURDATE()
      AND ABS(t.total_amount - IFNULL(y.total_amount, 0)) >= 500;
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def notify_funds_restriction(records):
    notifier, user_ids = get_notifier_and_users()

    for r in records:
        text = (
            f"【资金限制变动告警】\n"
            f"店铺：{r['店铺']}\n"
            f"限制原因：{r['限制原因']}\n"
            f"昨日金额：{r['yesterday_amount']}\n"
            f"今日金额：{r['today_amount']}\n"
            f"差值：{r['diff_amount']}"
        )
        logger.warning(text)

        notifier.send_text(
            user_ids=user_ids,
            title="Temu 资金限制变动",
            text=text
        )


# ======================
# 主入口
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
        # ① 违规
        violation_records = fetch_today_new_high_violation(conn)
        if violation_records:
            logger.warning(f"发现 {len(violation_records)} 条新增违规")
            notify_violation(violation_records)
        else:
            logger.info("今日无新增违规")

        # ② 资金限制
        restriction_records = fetch_today_diff_high_restriction(conn)
        if restriction_records:
            logger.warning(f"发现 {len(restriction_records)} 条资金限制异常")
            notify_funds_restriction(restriction_records)
        else:
            logger.info("今日无资金限制异常")

    finally:
        conn.close()


def run_send_data():
    main()


