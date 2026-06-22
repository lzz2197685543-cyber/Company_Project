# run_temu_reports.py
import asyncio
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from services.financial_report.full_temu_report import TemuFinancialReport
from services.financial_report.part_temu_report import PartTemuFinancialReport
from utils.logger import get_logger

logger = get_logger('financial_report')


async def run_full_temu(year_month=None):
    """运行Temu全托管报表"""
    from services.financial_report.full_temu_report import export_all_to_excel

    logger.info("\n" + "=" * 60)
    logger.info("开始运行 Temu全托管 财务报表")
    logger.info("=" * 60)

    shop_name_list = [
        "101-Temu全托管", "102-Temu全托管", "103-Temu全托管", "104-Temu全托管",
        "105-Temu全托管", "106-Temu全托管", "108-Temu全托管", "109-Temu全托管KA",
        "110-Temu全托管KA", "112-Temu全托管", "1101-Temu全托管", "1102-Temu全托管",
        "1103-Temu全托管", "1104-Temu全托管", "1105-Temu全托管", "1106-Temu全托管",
        "1107-Temu全托管", "1108-Temu全托管", "2101-Temu全托管KA", "2102-Temu全托管",
        "2103-Temu全托管", "2106-Temu全托管",
    ]

    all_reports = []
    first_year = None

    for shop_name in shop_name_list:
        report = TemuFinancialReport(
            shop_name=shop_name,
            frozen=0,
            use_cache=True,
            job='financial_report',
            year_month=year_month
        )
        await report.generate_full_report()
        all_reports.append(report.get_report_dict())
        if first_year is None:
            first_year = report.year

    if all_reports:
        export_all_to_excel(all_reports, shop_name_list, first_year)

    logger.info("Temu全托管报表完成")


async def run_part_temu(year_month=None):
    """运行Temu半托管报表"""
    from services.financial_report.part_temu_report import export_all_to_excel

    logger.info("\n" + "=" * 60)
    logger.info("开始运行 Temu半托管 财务报表")
    logger.info("=" * 60)

    shop_name_list = ["TEMU半托管-1号店", "TEMU半托管-3号店"]

    all_reports = []
    first_year = None

    for shop_name in shop_name_list:
        report = PartTemuFinancialReport(
            shop_name=shop_name,
            frozen=0,
            use_cache=True,
            job='financial_report',
            year_month=year_month
        )
        await report.generate_full_report()
        all_reports.append(report.get_report_dict())
        if first_year is None:
            first_year = report.year

    if all_reports:
        export_all_to_excel(all_reports, shop_name_list, first_year)

    logger.info("Temu半托管报表完成")


async def main():
    """主函数"""
    # year_month = "202605"  # 可以修改为指定月份，或设为None使用上个月

    # 运行全托管
    await run_full_temu()

    # 运行半托管
    await run_part_temu()

    logger.info("\n" + "=" * 60)
    logger.info("所有Temu报表运行完成")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())