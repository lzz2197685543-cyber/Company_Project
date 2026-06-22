# services/financial/financial_month_job.py

from services.financial.financial_data import Temu_Financial_Data
from services.financial.financial_checker import FinancialDataChecker
import asyncio
import time
from utils.logger import get_logger
from typing import Dict, List, Tuple
from datetime import datetime
from utils.config_loader import get_shop_config
from pathlib import Path
from services.financial.financial_process_up import financial_process_up
from utils.dingtalk_bot import ding_bot_send

FINANCIAL_DIR = Path(__file__).resolve().parent / "data" / "financial"

logger = get_logger("financial_data")

"""跑temu财务数据"""
def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"


# 计算前一个月的年份和月份
def get_prev_month_from_now() -> str:
    """
    返回当前时间的前一个月，格式：YYYY-MM
    """
    now = datetime.now()
    year = now.year
    month = now.month

    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1

    return f"{year}-{month:02d}"

async def check_all_financial_data(shop_names: List[str], month_str: str, send_dingtalk: bool = True):
    """
    检查所有店铺的财务数据导出情况

    Args:
        shop_names: 店铺名称列表
        month_str: 月份字符串
        send_dingtalk: 是否发送钉钉通知
    """
    checker = FinancialDataChecker(FINANCIAL_DIR)

    logger.info(f"开始检查 {month_str} 财务数据导出情况...")

    # 执行检查
    results = checker.check_all_shops(shop_names, month_str)

    # 生成报告
    report = checker.generate_report(results, month_str)
    logger.info(report)

    # 获取缺失文件列表
    missing_files = checker.get_missing_files_list(results)

    # 发送钉钉通知
    if send_dingtalk:
        # 发送完整报告
        ding_bot_send('me', f"📊 财务数据检查报告\n{report}")

        # 如果有缺失文件，单独发送提醒
        if missing_files:
            missing_msg = f"⚠️ 发现 {len(missing_files)} 个缺失文件，请检查以下店铺:\n"
            for shop_name, file_type in missing_files:
                missing_msg += f"• {shop_name}: 缺失 {file_type} 文件\n"
            ding_bot_send('me', missing_msg)

    # 返回检查结果，供后续处理使用
    return {
        "results": results,
        "missing_files": missing_files,
        "success_shops": sum(1 for status in results.values() if all(status.values())),
        "total_shops": len(shop_names)
    }

async def retry_failed_shops(shop_names: List[str], month_str: str, failed_shops: List[str] = None):
    """
    重新导出失败的店铺数据

    Args:
        shop_names: 所有店铺列表（用于从配置获取信息）
        month_str: 月份字符串
        failed_shops: 需要重试的店铺列表，如果为None则自动检测
    """
    if failed_shops is None:
        # 自动检测失败的店铺
        checker = FinancialDataChecker(FINANCIAL_DIR)
        results = checker.check_all_shops(shop_names, month_str)
        failed_shops = [
            shop_name for shop_name, status in results.items()
            if not all(status.values())
        ]

    if not failed_shops:
        logger.info("✅ 没有失败的店铺需要重试")
        return

    logger.info(f"开始重新导出失败的店铺: {failed_shops}")

    for shop_name in failed_shops:
        try:
            account = get_shop_config(shop_name)
            t = Temu_Financial_Data(shop_name, account, month_str, 'financial_data_retry')
            success = await t.run()

            if success:
                logger.info(f"✅ {shop_name} 重新导出成功")
            else:
                logger.error(f"❌ {shop_name} 重新导出失败")
                ding_bot_send('me', f"❌ {shop_name} 重新导出失败，需要人工处理")

        except Exception as e:
            logger.error(f"重新导出 {shop_name} 时出错: {e}")
            ding_bot_send('me', f"❌ {shop_name} 重新导出异常: {str(e)}")

    # 重新检查
    logger.info("重新导出完成，开始验证...")
    await check_all_financial_data(shop_names, month_str, send_dingtalk=True)


async def main_all():
    """原有的下载流程（不带检查）"""
    month_str = get_prev_month_from_now()


    logger.info(f'--------------------------正在下载{month_str}的数据------------------------------')
    shop_name_list = [
        "2106-Temu全托管", "2103-Temu全托管", "2102-Temu全托管", "2101-Temu全托管KA",
        "112-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管", "1105-Temu全托管", "1104-Temu全托管","109-Temu全托管KA",
        "1103-Temu全托管", "1102-Temu全托管", "1101-Temu全托管",
        "110-Temu全托管KA", "108-Temu全托管", "106-Temu全托管", "105-Temu全托管",
        "104-Temu全托管", "103-Temu全托管", "102-Temu全托管", "101-Temu全托管",
    ]

    # 顺序处理
    for shop_name in shop_name_list:
        logger.info(f'--------------正在爬取店铺{shop_name}----的数据')
        account = get_shop_config(shop_name)
        t = Temu_Financial_Data(shop_name, account, month_str, 'financial_data')
        await t.run()

    # 处理数据
    logger.info(f'--------------------------正在处理数据------------------------------')
    filepath = FINANCIAL_DIR / f"{month_str.split('-')[1]}月份"
    financial_process_up(filepath, f"{month_str.split('-')[0]}年{month_str.split('-')[1]}月")

async def main_all_with_check():
    """带检查功能的主流程（推荐使用）"""
    month_str = get_prev_month_from_now()
    # month_str = '2026-01'

    shop_name_list = [
        "2106-Temu全托管", "2103-Temu全托管", "2102-Temu全托管", "2101-Temu全托管KA",
        "112-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管", "1105-Temu全托管", "1104-Temu全托管","109-Temu全托管KA",
        "1103-Temu全托管", "1102-Temu全托管", "1101-Temu全托管",
        "110-Temu全托管KA",  "108-Temu全托管", "106-Temu全托管", "105-Temu全托管",
        "104-Temu全托管", "103-Temu全托管", "102-Temu全托管", "101-Temu全托管",
    ]
    # shop_name_list = ["1103-Temu全托管"]

    total_start = time.perf_counter()

    # 第一步：下载数据
    logger.info(f'--------------------------正在下载{month_str}的数据------------------------------')

    for shop_name in shop_name_list:
        logger.info(f'--------------正在爬取店铺{shop_name}----的数据')
        account = get_shop_config(shop_name)
        t = Temu_Financial_Data(shop_name, account, month_str, 'financial_data')
        await t.run()

    # 第二步：检查下载结果
    logger.info(f'--------------------------正在检查{month_str}的数据导出情况------------------------------')
    check_result = await check_all_financial_data(shop_name_list, month_str, send_dingtalk=True)

    total_cost = time.perf_counter() - total_start

    # 发送最终报告
    final_msg = f"temu_financial任务完成\n"
    final_msg += f"总耗时：{format_seconds(total_cost)}\n"
    final_msg += f"总店铺数：{check_result['total_shops']}\n"
    final_msg += f"成功店铺数：{check_result['success_shops']}\n"
    final_msg += f"失败店铺数：{check_result['total_shops'] - check_result['success_shops']}"

    if check_result['missing_files']:
        final_msg += f"\n缺失文件数：{len(check_result['missing_files'])}"

    ding_bot_send('me', final_msg)
    logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")

    # 第三步：处理数据
    # logger.info(f'--------------------------正在处理数据------------------------------')
    # filepath = FINANCIAL_DIR / f"{month_str.split('-')[1]}月份"
    # financial_process_up(filepath, f"{month_str.split('-')[0]}年{month_str.split('-')[1]}月")

    # 可选：自动重试失败的店铺（取消注释即可启用）
    if check_result['total_shops'] - check_result['success_shops'] > 0:
        logger.info("开始自动重试失败的店铺...")
        await retry_failed_shops(shop_name_list, month_str)

    return check_result

async def check_only():
    """仅检查，不下载"""
    month_str = get_prev_month_from_now()


    shop_name_list = [
        "2106-Temu全托管", "2103-Temu全托管", "2102-Temu全托管", "2101-Temu全托管KA",
        "112-Temu全托管",
        "1108-Temu全托管", "1107-Temu全托管", "1106-Temu全托管", "1105-Temu全托管", "1104-Temu全托管",
        "1103-Temu全托管", "1102-Temu全托管", "1101-Temu全托管",
        "110-Temu全托管KA", "109-Temu全托管KA", "108-Temu全托管", "106-Temu全托管", "105-Temu全托管",
        "104-Temu全托管", "103-Temu全托管", "102-Temu全托管", "101-Temu全托管",
    ]

    await check_all_financial_data(shop_name_list, month_str, send_dingtalk=True)


if __name__ == '__main__':
    # 推荐：运行完整流程（下载+检查+处理数据）
    asyncio.run(main_all_with_check())

    # 或者：只运行原有流程（下载+处理数据，不检查）
    # asyncio.run(main_all())

    # 或者：只运行检查
    # asyncio.run(check_only())