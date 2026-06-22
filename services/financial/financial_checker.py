
# 在文件末尾添加以下类和函数
import time
import asyncio
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from services.financial.financial_data import Temu_Financial_Data, FINANCIAL_DIR
from utils.logger import get_logger
from utils.config_loader import get_shop_config
from utils.dingtalk_bot import ding_bot_send


class FinancialDataChecker:
    """财务数据导出检查器"""

    def __init__(self, financial_dir: Path):
        self.financial_dir = financial_dir
        self.logger = get_logger("financial_checker")

    def check_shop_files(self, shop_name: str, month_str: str) -> Dict[str, bool]:
        """
        检查单个店铺的所有财务文件是否都存在

        Args:
            shop_name: 店铺名称
            month_str: 月份字符串，格式: YYYY-MM

        Returns:
            包含各文件存在状态的字典
        """
        month_num = month_str.split('-')[1]
        month_folder = self.financial_dir / f"{month_num}月份"

        # 定义需要检查的文件和后缀
        file_checks = {
            "卖家中心": f"temu/{shop_name}_{month_num}_卖家中心.xlsx",
            "全球": f"{shop_name}_{month_num}_全球.xlsx",
            "欧区": f"{shop_name}_{month_num}_欧区.xlsx",
            "美国": f"{shop_name}_{month_num}_美国.xlsx"
        }

        result = {}
        for file_type, filename in file_checks.items():
            file_path = month_folder / filename
            result[file_type] = file_path.exists()

        return result

    def check_all_shops(self, shop_names: List[str], month_str: str) -> Dict[str, Dict[str, bool]]:
        """
        检查所有店铺的文件导出情况

        Args:
            shop_names: 店铺名称列表
            month_str: 月份字符串

        Returns:
            所有店铺的文件检查结果
        """
        all_results = {}
        for shop_name in shop_names:
            all_results[shop_name] = self.check_shop_files(shop_name, month_str)
        return all_results

    def generate_report(self, results: Dict[str, Dict[str, bool]], month_str: str) -> str:
        """
        生成检查报告

        Args:
            results: 检查结果
            month_str: 月份字符串

        Returns:
            格式化的报告字符串
        """
        month_num = month_str.split('-')[1]
        report_lines = []
        report_lines.append(f"\n{'= ' *60}")
        report_lines.append(f"📊 {month_str} 财务数据导出检查报告")
        report_lines.append(f"{'= ' *60}\n")

        total_shops = len(results)
        success_shops = 0
        all_files_success_shops = []
        failed_shops = []

        for shop_name, file_status in results.items():
            success_count = sum(file_status.values())
            total_count = len(file_status)

            if success_count == total_count:
                success_shops += 1
                all_files_success_shops.append(shop_name)
                report_lines.append(f"✅ {shop_name}: 全部文件已导出 ({success_count}/{total_count})")
            else:
                failed_shops.append(shop_name)
                report_lines.append(f"⚠️ {shop_name}: 导出不完整 ({success_count}/{total_count})")
                # 列出缺失的文件
                missing_files = [f_type for f_type, exists in file_status.items() if not exists]
                for missing in missing_files:
                    report_lines.append(f"   ❌ 缺失: {missing}")
                report_lines.append("")

        # 汇总信息
        report_lines.append(f"\n{'= ' *60}")
        report_lines.append(f"📈 统计汇总:")
        report_lines.append(f"   总店铺数: {total_shops}")
        report_lines.append(f"   全部成功: {success_shops}")
        report_lines.append(f"   部分失败: {len(failed_shops)}")
        report_lines.append(f"   成功率: {success_shops /total_shops *100:.1f}%")

        if all_files_success_shops:
            report_lines.append(f"\n🎉 全部文件导出成功的店铺 ({len(all_files_success_shops)}个):")
            for shop in all_files_success_shops:
                report_lines.append(f"   • {shop}")

        if failed_shops:
            report_lines.append(f"\n⚠️ 需要重新导出的店铺 ({len(failed_shops)}个):")
            for shop in failed_shops:
                report_lines.append(f"   • {shop}")

        report_lines.append(f"\n{'= ' *60}")
        report_lines.append(f"📁 文件存储路径: {self.financial_dir / f'{month_num}月份' / 'temu'}")
        report_lines.append(f"{'= ' *60}\n")

        return "\n".join(report_lines)

    def get_missing_files_list(self, results: Dict[str, Dict[str, bool]]) -> List[Tuple[str, str]]:
        """
        获取所有缺失的文件列表

        Returns:
            缺失文件列表，每个元素为 (店铺名称, 文件类型)
        """
        missing_files = []
        for shop_name, file_status in results.items():
            for file_type, exists in file_status.items():
                if not exists:
                    missing_files.append((shop_name, file_type))
        return missing_files


