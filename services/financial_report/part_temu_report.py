import os
import pickle
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple
from utils.logger import get_logger
from utils.load_sku_mapping import load_cost_mapping, get_cost_price
from services.financial_report.verify_stockin_manager import VerifyStockinManager
from utils.dingding_doc import upload_multiple_records, DingTalkTokenManager, DingTalkSheetQuery
from utils.dingding_collector import DingTalkDataQuery,DingTalkDailyDataQuery


class PartTemuFinancialReport:
    """Temu半托管财务报表生成类"""

    def __init__(self, job: str, shop_name: str, frozen: float = 0, use_cache: bool = True,
                 year_month: Optional[str] = None):
        """
        初始化Temu半托管财务报表

        参数:
            job: 任务名称
            shop_name: 店铺名称，如 "TEMU半托管-1号店"
            frozen: 冻结金额（负数形式）
            use_cache: 是否使用成本映射缓存
        """
        self.shop_name = shop_name
        self.frozen = frozen
        self.use_cache = use_cache
        self.job = job
        self.logger = get_logger(job)

        # 获取月份信息（支持传入）
        self.year, self.month = self._get_target_month(year_month)
        self.month_dir = self.month + '月份'

        # 设置文件路径
        self.file_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "financial" / self.month_dir / "part_data"
        self.file_dir1 = Path(__file__).resolve().parent.parent.parent.parent / "data"

        # 初始化结果字典
        self.report = {
            '店铺': self.shop_name,
            '销售收入': 0,
            '交易收入': 0,
            '运费收入': 0,
            '售后退款': 0,
            '运费退款': 0,
            '罚款': 0,
            '冻结': 0,
            '销售成本': 0,
            '自发货成本': 0,
            '亚马逊代发货成本': 0,
            '毛利额': 0,
            '销售费用': 0,
            '头程运费': 0,
            '平台赔付': 0,
            '发货面单费': 0,
            '退货面单费': 0,
            '西邮运费': 0,
            '至美通运费': 0,
            '国内运费': 0,
            '包装材料费': 0,
            '公关费': 0,
            '其他': 0,
            '经营利润': 0,
        }

        # 成本映射缓存
        self.single_sku_cost = {}
        self.combined_sku_cost = {}

    def _get_target_month(self, year_month: Optional[str]) -> Tuple[str, str]:
        """
        获取目标月份

        参数:
            year_month: 年月，格式 YYYYMM，如 "202605"

        返回:
            (YYYYMM, MM) 格式的元组
        """
        if year_month and len(year_month) == 6:
            # 使用传入的年月
            year = year_month
            month = year_month[4:6]
            self.logger.info(f"使用指定年月: {year}, 月份: {month}")
            return year, month

        # 默认使用上个月
        now = datetime.now()
        year = now.year
        month = now.month

        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1

        year_str = f"{year}{month:02d}"
        month_str = f"{month:02d}"
        self.logger.info(f"使用上月: {year_str}, 月份: {month_str}")
        return year_str, month_str

    @staticmethod
    def _get_prev_month_from_now() -> tuple:
        """返回当前时间的前一个月，返回两个值：(YYYYMM, MM)"""
        now = datetime.now()
        year = now.year
        month = now.month

        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1

        return f"{year}{month:02d}", f"{month:02d}"

    async def calculate_revenue(self):
        """计算半托管销售收入相关数据"""
        us_file = self.file_dir / f'{self.shop_name}_美国.xlsx'
        seller_center_file = self.file_dir / f'{self.shop_name}_卖家中心.xlsx'

        # 1. 处理美国结算表
        self.logger.info(f"\n正在处理美国结算表: {us_file.name}")
        try:
            df_us = pd.read_excel(us_file, sheet_name='结算')
            self.logger.info(f"美国结算表列名: {df_us.columns.tolist()}")

            self.report['交易收入'] = round(df_us[df_us['交易类型'] == '销售回款']['结算金额'].sum(), 2)
            self.report['运费收入'] = round(df_us[df_us['交易类型'] == '运费回款']['结算金额'].sum(), 2)
            self.report['售后退款'] = round((df_us[df_us['交易类型'] == '销售冲回']['结算金额'].sum()), 2)
            self.report['运费退款'] = round((df_us[df_us['交易类型'] == '运费冲回']['结算金额'].sum()), 2)

            self.logger.info(f"  交易收入: {self.report['交易收入']:.2f}")
            self.logger.info(f"  运费收入: {self.report['运费收入']:.2f}")
            self.logger.info(f"  售后退款: {self.report['售后退款']:.2f}")
            self.logger.info(f"  运费退款: {self.report['运费退款']:.2f}")

        except FileNotFoundError:
            self.logger.error(f"错误：找不到文件 {us_file}")
        except Exception as e:
            self.logger.error(f"错误：{e}")

        # 2. 处理卖家中心表（罚款）
        self.logger.info(f"\n正在处理卖家中心表: {seller_center_file.name}")
        try:
            df_sc = pd.read_excel(seller_center_file)
            penalty_mask = (df_sc['账务类型'] == '支出') & (df_sc['备注'].str.contains('发货履约保障', na=False))
            penalty_amount = df_sc[penalty_mask]['收支金额'].sum()
            self.report['罚款'] = round((penalty_amount), 2)
            self.logger.info(f"  罚款: {self.report['罚款']:.2f}")

        except FileNotFoundError:
            self.logger.error(f"错误：找不到文件 {seller_center_file}")
        except Exception as e:
            self.logger.error(f"错误：{e}")

        self.report['冻结'] = self.frozen

    async def calculate_self_delivery_cost(self):
        """计算自发货成本：从结算表筛选销售回款，匹配SKU成本价 * 件数"""
        self.logger.info("\n正在加载成本映射表...")
        self.single_sku_cost, self.combined_sku_cost = load_cost_mapping(use_cache=self.use_cache)

        if not self.single_sku_cost and not self.combined_sku_cost:
            self.logger.error("错误：未能加载任何成本映射数据")
            self.report['自发货成本'] = 0
            return

        files_to_process = [
            ('美国', self.file_dir / f'{self.shop_name}_美国.xlsx'),
            ('全球', self.file_dir / f'{self.shop_name}_全球.xlsx'),
        ]

        total_cost = 0.0
        total_matched = 0
        total_unmatched = 0
        all_unmatched_skus = set()

        for region_name, file_path in files_to_process:
            region_cost = 0.0
            matched_count = 0
            unmatched_skus = set()

            self.logger.info(f"\n正在处理 {region_name} 结算表: {file_path.name}")

            try:
                if not file_path.exists():
                    self.logger.info(f"  {region_name} 文件不存在，跳过")
                    continue

                # 检查是否有"结算"子表
                try:
                    excel_file = pd.ExcelFile(file_path)
                    if '结算' not in excel_file.sheet_names:
                        self.logger.info(f"  {region_name} 文件中没有'结算'子表，跳过")
                        continue
                except Exception as e:
                    self.logger.warning(f"  {region_name} 读取子表列表失败: {e}")
                    continue

                # 读取结算子表，不跳过空行
                df = pd.read_excel(file_path, sheet_name='结算', header=None)

                # 找到SKU货号和件数的列位置
                sku_column = None
                quantity_column = None

                # 查找表头行
                for row_idx in range(min(3, len(df))):
                    for col_idx, val in enumerate(df.iloc[row_idx]):
                        if pd.isna(val):
                            continue
                        val_str = str(val).strip()
                        if 'SKU货号' in val_str:
                            sku_column = col_idx
                            self.logger.info(f"  找到SKU货号列: 第{col_idx}列")
                        if '件数' in val_str:
                            quantity_column = col_idx
                            self.logger.info(f"  找到件数列: 第{col_idx}列")

                if sku_column is None or quantity_column is None:
                    self.logger.error(f"  无法找到SKU货号列或件数列")
                    continue

                # 找到交易类型列的位置
                transaction_column = None
                for col_idx, val in enumerate(df.iloc[0]):
                    if pd.isna(val):
                        continue
                    if '交易类型' in str(val):
                        transaction_column = col_idx
                        break

                if transaction_column is None:
                    self.logger.error(f"  无法找到交易类型列")
                    continue

                # 筛选销售回款记录
                sales_orders = []
                for row_idx in range(len(df)):
                    if row_idx < 2:
                        continue
                    transaction_type = df.iloc[row_idx, transaction_column]
                    if transaction_type == '销售回款':
                        sales_orders.append(df.iloc[row_idx])

                self.logger.info(f"  共筛选出 {len(sales_orders)} 条销售回款记录")

                for row in sales_orders:
                    sku_attr = row[sku_column]
                    quantity = row[quantity_column]

                    if pd.isna(sku_attr):
                        continue

                    if pd.isna(quantity):
                        quantity = 0
                    else:
                        quantity = float(quantity)

                    cost = get_cost_price(sku_attr, self.single_sku_cost, self.combined_sku_cost)

                    if cost is not None and not pd.isna(cost):
                        region_cost += float(cost) * quantity
                        matched_count += 1
                    else:
                        unmatched_skus.add(sku_attr)

                if unmatched_skus:
                    unmatched_list = list(unmatched_skus)
                    self.logger.warning(f"  {region_name} 以下SKU未匹配: {unmatched_list[:5]}")
                    if len(unmatched_skus) > 5:
                        self.logger.warning(f"  ... 共 {len(unmatched_skus)} 个SKU未匹配")
                    all_unmatched_skus.update(unmatched_skus)

                self.logger.info(f"  {region_name} 匹配成功 {matched_count} 条，未匹配 {len(unmatched_skus)} 条")
                self.logger.info(f"  {region_name} 自发货成本: {region_cost:.2f}")

                total_cost += region_cost
                total_matched += matched_count
                total_unmatched += len(unmatched_skus)

            except Exception as e:
                self.logger.error(f"  {region_name} 错误：{e}")

        self.report['自发货成本'] = round(total_cost, 2)
        self.logger.info(f"\n自发货成本总计: {self.report['自发货成本']:.2f}")
        self.logger.info(f"总匹配成功 {total_matched} 条，未匹配 {total_unmatched} 条")

    async def calculate_seller_center_expenses(self):
        """计算卖家中心表中的各项费用：平台赔付、发货面单费、退货面单费"""
        seller_center_file = self.file_dir / f'{self.shop_name}_卖家中心.xlsx'
        self.logger.info(f"\n正在处理卖家中心表: {seller_center_file.name}")

        try:
            df = pd.read_excel(seller_center_file)
            self.logger.info(f"卖家中心表列名: {df.columns.tolist()}")

            # 平台赔付
            compensation_amount = df[df['账务类型'] == '赔付']['收支金额'].sum()
            self.report['平台赔付'] = round(abs(compensation_amount)*-1, 2)

            # 发货面单费：模糊匹配，备注包含"发货面单费"
            shipping_label_mask = df['备注'].str.contains('发货面单费', na=False)
            shipping_label_amount = df[shipping_label_mask]['收支金额'].sum()
            self.report['发货面单费'] = round(abs(shipping_label_amount), 2)

            # 退货面单费：模糊匹配，备注包含"退货面单费"
            return_label_mask = df['备注'].str.contains('退货面单费', na=False)
            return_label_amount = df[return_label_mask]['收支金额'].sum()
            self.report['退货面单费'] = round(abs(return_label_amount), 2)

            self.logger.info("\n卖家中心费用计算结果：")
            self.logger.info(f"  平台赔付: {self.report['平台赔付']:.2f}")
            self.logger.info(f"  发货面单费: {self.report['发货面单费']:.2f}")
            self.logger.info(f"  退货面单费: {self.report['退货面单费']:.2f}")

        except FileNotFoundError:
            self.logger.error(f"错误：找不到文件 {seller_center_file}")
        except Exception as e:
            self.logger.error(f"错误：{e}")

    async def calculate_daily_bookkeeping(self, target_month=None):
        """计算日常记账费用（优化版）"""
        target_month = self.month
        # 查询数据
        daily_query = DingTalkDailyDataQuery(target_month=target_month)
        df = daily_query.load_data(force_refresh=False)

        if df.empty:
            self.logger.warning("没有日常记账数据")
            return {k: 0 for k in ['西邮运费', '至美通运费', '国内运费', '包装材料费', '公关费', '其他']}

        # 数据预处理：统一字段格式
        # 处理店铺字段
        if '店铺名称' in df.columns:
            df['店铺'] = df['店铺名称']
        elif '归属店铺' in df.columns:
            df['店铺'] = df['归属店铺'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else str(x))

        # 处理支出项目字段
        if '支出项目名称' in df.columns:
            df['支出项目'] = df['支出项目名称']
        elif '支出项目' in df.columns:
            df['支出项目'] = df['支出项目'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else str(x))

        # 处理金额字段
        if '金额_数值' in df.columns:
            df['金额'] = pd.to_numeric(df['金额_数值'], errors='coerce').fillna(0)
        elif '金额' in df.columns:
            df['金额'] = pd.to_numeric(df['金额'], errors='coerce').fillna(0)

        # 筛选店铺并分组聚合
        shop_df = df[df['店铺'] == self.shop_name]
        if shop_df.empty:
            self.logger.warning(f"未找到店铺 '{self.shop_name}' 的数据")
            return {k: 0 for k in ['西邮运费', '至美通运费', '国内运费', '包装材料费', '公关费', '其他']}

        expense_summary = shop_df.groupby('支出项目')['金额'].sum().round(2)

        # 初始化费用并填充
        daily_map = {'西邮运费':0, '至美通运费':0, '国内运费':0, '包装材料费':0, '公关费':0, '其他':0}
        for item, amount in expense_summary.items():
            if item in daily_map:
                daily_map[item] = amount
            else:
                daily_map['其他'] += amount

        # 更新报告
        for key in daily_map:
            self.report[key] = round(daily_map[key], 2)

        # 输出日志
        self.logger.info(f"\n日常记账费用（{target_month or '上月'}）：")
        for key, value in daily_map.items():
            if value:
                self.logger.info(f"  {key}: {value:.2f}")
        self.logger.info(f"  合计: {sum(daily_map.values()):.2f}")

        return daily_map

    def calculate_profit(self):
        """计算销售收入、销售成本、毛利额、销售费用、经营利润"""
        # 销售收入 = 交易收入 + 运费收入 - 售后退款 - 运费退款 - 罚款 - 冻结
        self.report['销售收入'] = round(
            self.report['交易收入'] + self.report['运费收入']
            + self.report['售后退款'] + self.report['运费退款']
            + self.report['罚款'] + self.report['冻结'], 2
        )

        # 销售成本 = 自发货成本 + 亚马逊代发货成本
        self.report['销售成本'] = round(self.report['自发货成本'] + self.report['亚马逊代发货成本'], 2)

        # 毛利额 = 销售收入 - 销售成本
        self.report['毛利额'] = round(self.report['销售收入'] - self.report['销售成本'], 2)

        # 销售费用
        expense_fields = ['头程运费', '平台赔付', '发货面单费', '退货面单费', '西邮运费', '至美通运费', '国内运费', '包装材料费', '公关费', '其他']
        self.report['销售费用'] = round(sum(self.report[f] for f in expense_fields), 2)

        # 经营利润 = 毛利额 - 销售费用
        self.report['经营利润'] = round(self.report['毛利额'] - self.report['销售费用'], 2)

        self.logger.info(f"\n销售收入: {self.report['销售收入']:.2f}")
        self.logger.info(f"销售成本: {self.report['销售成本']:.2f}")
        self.logger.info(f"毛利额: {self.report['毛利额']:.2f}")
        self.logger.info(f"销售费用: {self.report['销售费用']:.2f}")
        self.logger.info(f"经营利润: {self.report['经营利润']:.2f}")

    async def generate_full_report(self):
        """生成完整财务报表"""
        print("=" * 60)
        self.logger.info(f"开始生成 {self.shop_name} {self.year} 半托管财务报表")
        print("=" * 60)

        await self.calculate_revenue()
        await self.calculate_self_delivery_cost()
        await self.calculate_seller_center_expenses()
        await self.calculate_daily_bookkeeping()
        self.calculate_profit()

        print("\n" + "=" * 60)
        self.logger.info("财务报表生成完成")
        print("=" * 60)

        return self.report

    def get_report_dict(self):
        return self.report


def export_all_to_excel(all_reports: list, shop_name_list: list, year: str, output_path: str = None):
    """导出所有店铺报表到Excel（指标作为行，店铺作为列）"""
    path = Path(
        __file__).resolve().parent.parent.parent.parent / "data" / "financial" / f"{year[4:6]}月份" / "part_report"
    path.mkdir(parents=True, exist_ok=True)
    if output_path is None:
        output_path = path / f"temu半托_财务报表_{year}.xlsx"

    report_order = [
        '销售收入', '交易收入', '运费收入', '售后退款', '运费退款', '罚款', '冻结',
        '销售成本', '自发货成本', '亚马逊代发货成本',
        '毛利额', '销售费用',
        '头程运费', '平台赔付', '发货面单费', '退货面单费',
        '西邮运费', '至美通运费', '国内运费', '包装材料费', '公关费', '其他',
        '经营利润'
    ]

    data = {'店铺': report_order}
    for i, shop_name in enumerate(shop_name_list):
        data[shop_name] = [all_reports[i].get(key, 0) for key in report_order]

    df = pd.DataFrame(data)
    df.to_excel(output_path, index=False)
    print(f"\n报表已导出到: {output_path}")
    return output_path


async def main():
    shop_name_list = ["TEMU半托管-1号店", "TEMU半托管-3号店"]
    all_reports = []
    first_year = None

    for shop_name in shop_name_list:
        report = PartTemuFinancialReport(
            shop_name=shop_name,
            frozen=0,
            use_cache=True,
            job='financial_report',
            year_month="202605"
        )
        await report.generate_full_report()
        all_reports.append(report.get_report_dict())
        if first_year is None:
            first_year = report.year

    if all_reports:
        export_all_to_excel(all_reports, shop_name_list, first_year)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())