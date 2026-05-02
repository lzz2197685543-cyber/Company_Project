import asyncio
from pathlib import Path
import pandas as pd
from datetime import datetime,timedelta

from services.Chinese_Financial_Statements.crawl_payment_cost import PaymentCostClient
from services.Chinese_Financial_Statements.crawl_refund_amount_cost import RefundAmountClient
from services.Chinese_Financial_Statements.crawl_daily_bookkeeping import DailyBookkeeping
from services.Chinese_Financial_Statements.detail_tianmao import TianMao_FinancialDetailService

# ==================== 店铺配置 ====================
SHOP_MAP = {
    "耀乐玩具旗舰店-天猫": "14622509"
}

# 账务明细只跑线上两个店铺
ONLINE_DETAIL_SHOPS = [
    "耀乐玩具旗舰店-天猫"
]
target_shops = [
    "耀乐玩具旗舰店-天猫"
]

# ==================== 费用项 ====================
EXPENSE_ROWS = [
    "代扣返点积分",
    "代扣交易退回积分",
    "天猫保证金-延迟发货",
    "天猫保证金-虚假发货",
    "淘宝天猫跨境服务增值费",
    "天猫保证金-缺货",
    "天猫保证金-物流轨迹超时",
    "天猫佣金",
    "品牌新享-首单拉新计划",
    "天猫保证金-退货邮费",
    "基础软件服务费",
    "商家集运物流服务费",
    "商家集运中转操作费",
    "F15110001 广告推广费",
    "F15110002 国内运费",
    "F15110003 软件费用",
    "F15110007 样品费",
    "F15110013 包装材料费",
    "F15110023 单号费",
    "F15110099 其他",
]

# ==================== 报表结构 ====================
REPORT_ROWS = [
    "销售收入",
    "付款金额",
    "退款金额",
    "销售成本",
    "商品成本",
    "退款成本",
    "毛利额",
    "经营费用",
    *EXPENSE_ROWS,
    "经营利润",
]


# ==================== 工具函数 ====================
def to_float(v):
    """统一转 float"""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0

def get_previous_month_range(date_format: str = "%Y-%m-%d") -> tuple:
    """获取当前时间前一个月的整月时间范围（结束日期+1天）"""
    today = datetime.now()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    end_date_plus_one = last_day_of_previous_month

    start_date = first_day_of_previous_month.strftime(date_format)
    end_date = end_date_plus_one.strftime(date_format)
    return start_date, end_date


def get_last_month_period():
    """
    返回上个月，格式：YYYYMM
    """
    today = datetime.today()

    year = today.year
    month = today.month

    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1

    return f"{year}{month:02d}"


# ==================== 数据获取 ====================
async def fetch_shop_base_data(shop_id, start_date=None, end_date=None):
    payment_client = PaymentCostClient("financial_statement", shop_id, start_date, end_date)
    refund_client = RefundAmountClient("financial_statement", shop_id, start_date, end_date)

    payment_res = await payment_client.main()
    refund_res = await refund_client.main()

    print({
        "付款金额": to_float(payment_res.get("sales_amount")),
        "商品成本": to_float(payment_res.get("cost_of_sales")),
        "退款金额": to_float(refund_res.get("refund_amount")),
        "退款成本": to_float(refund_res.get("refund_cost")),
    })

    return {
        "付款金额": to_float(payment_res.get("sales_amount")),
        "商品成本": to_float(payment_res.get("cost_of_sales")),
        "退款金额": to_float(refund_res.get("refund_amount")),
        "退款成本": to_float(refund_res.get("refund_cost")),
    }


async def fetch_bookkeeping_expenses(start_date=None, end_date=None):
    """获取费用流水（F开头项目）"""
    result = {shop: {} for shop in SHOP_MAP.keys()}

    client = DailyBookkeeping("financial_statement", target_shops, start_date, end_date)
    summary_df, _ = await client.get_money()

    if summary_df is None:
        return result

    for shop_name in summary_df.index:
        for col in summary_df.columns:
            result[shop_name][col] = to_float(summary_df.loc[shop_name, col])

    print(result)

    return result


def fetch_financial_detail_expenses(period):
    """获取账务明细费用（只线上）"""
    service = TianMao_FinancialDetailService()
    return service.get_all_shop_summary(ONLINE_DETAIL_SHOPS, period=period)


def merge_expenses(bookkeeping_expenses, detail_expenses):
    """合并两类费用"""
    result = {shop: {} for shop in SHOP_MAP.keys()}

    for shop in SHOP_MAP.keys():
        for fee_name, amount in bookkeeping_expenses.get(shop, {}).items():
            result[shop][fee_name] = result[shop].get(fee_name, 0) + to_float(amount)

        for fee_name, amount in detail_expenses.get(shop, {}).items():
            result[shop][fee_name] = result[shop].get(fee_name, 0) + to_float(amount)

    return result


# ==================== 核心报表 ====================
async def build_report(start_date=None, end_date=None, period=None):
    report = pd.DataFrame(
        0.0,
        index=REPORT_ROWS,
        columns=[*SHOP_MAP.keys(), "合计"]
    )

    # === 1. 基础数据 ===
    base_results = []
    for shop_name, shop_id in SHOP_MAP.items():
        print(f"开始获取 {shop_name} 基础数据...")
        result = await fetch_shop_base_data(shop_id, start_date, end_date)
        base_results.append(result)

    for (shop_name, _), base_data in zip(SHOP_MAP.items(), base_results):
        report.loc["付款金额", shop_name] = base_data["付款金额"]
        report.loc["退款金额", shop_name] = base_data["退款金额"]
        report.loc["商品成本", shop_name] = base_data["商品成本"]
        report.loc["退款成本", shop_name] = base_data["退款成本"]

        report.loc["销售收入", shop_name] = base_data["付款金额"] - base_data["退款金额"]
        report.loc["销售成本", shop_name] = base_data["商品成本"] - base_data["退款成本"]
        report.loc["毛利额", shop_name] = report.loc["销售收入", shop_name] - report.loc["销售成本", shop_name]

    # === 2. 费用 ===
    bookkeeping_expenses = await fetch_bookkeeping_expenses(start_date, end_date)
    detail_expenses = fetch_financial_detail_expenses(period)
    all_expenses = merge_expenses(bookkeeping_expenses, detail_expenses)

    for shop_name in SHOP_MAP.keys():
        total = 0
        for fee_name in EXPENSE_ROWS:
            val = to_float(all_expenses.get(shop_name, {}).get(fee_name, 0))
            report.loc[fee_name, shop_name] = val
            total += val

        report.loc["经营费用", shop_name] = total
        report.loc["经营利润", shop_name] = report.loc["毛利额", shop_name] - total

    # === 3. 合计 ===
    for row in REPORT_ROWS:
        report.loc[row, "合计"] = report.loc[row, list(SHOP_MAP.keys())].sum()

    return report.round(2)


# ==================== Excel 美化 ====================
def export_pretty_excel(df, output_file):
    from openpyxl import load_workbook
    from openpyxl.styles import Font, Alignment

    df.to_excel(output_file)

    wb = load_workbook(output_file)
    ws = wb.active

    # 加粗 + 红色字体的行
    bold_rows = ["销售收入", "销售成本", "毛利额", "经营费用", "经营利润"]

    for row in ws.iter_rows(min_row=2):
        row_name = row[0].value
        if row_name in bold_rows:
            for cell in row:
                cell.font = Font(bold=True, color="FF0000")  # 红色 + 加粗

    # 列宽优化
    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max_length + 4

    # 全表左对齐（你要求的）
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(horizontal="left", vertical="center")

    wb.save(output_file)


# ==================== 主函数 ====================
async def main():
    start_date = "2026-03-01"
    end_date = "2026-03-31"
    period = "202603"

    # start_date,end_date=get_previous_month_range()
    # period = get_last_month_period()


    report = await build_report(start_date,end_date,period,)

    print("\n========== 财务报表 ==========")
    print(report)

    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f"financial_statement_{period}_天猫.xlsx"

    export_pretty_excel(report, output_file)

    print(f"\n报表已保存: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
