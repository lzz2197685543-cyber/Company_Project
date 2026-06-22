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
from utils.dingding_collector import DingTalkDailyDataQuery
from utils.dingtalk_bot import ding_bot_send



class TemuFinancialReport:
    """Temu财务报表生成类"""

    def __init__(self, job: str, shop_name: str, frozen: float = 0, use_cache: bool = True,
                 year_month: Optional[str] = None):
        """
        初始化Temu财务报表

        参数:
            job: 任务名称
            shop_name: 店铺名称，如 "101-Temu全托管"
            frozen: 冻结金额（负数形式）
            use_cache: 是否使用成本映射缓存
            year_month: 年月，格式 YYYYMM，如 "202605"。不传则默认上个月
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
        self.file_dir = Path(__file__).resolve().parent.parent.parent / "data" / "financial" / self.month_dir / "temu"
        self.file_dir1 = Path(__file__).resolve().parent.parent.parent.parent / "data"

        # 初始化结果字典
        self.report = {
            '店铺': self.shop_name,
            '销售收入': 0,
            '订单金额': 0,
            '非商责补贴': 0,
            '平台退款': 0,
            '冻结': 0,
            '销售成本': 0,
            '订单成本': 0,
            '丢件': 0,
            '毛利额': 0,
            '销售费用': 0,
            '平台罚款-售后': 0,
            '平台仓储费': 0,
            'EPR环保费': 0,
            '平台罚款': 0,
            '广告服务费': 0,
            '国内运费': 0,
            '样品费': 0,
            '服务费': 0,
            '包装材料费': 0,
            '单号费': 0,
            '其他': 0,
            '经营利润': 0
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
        """
        返回当前时间的前一个月，返回两个值：(YYYYMM, MM)
        """
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
        """计算销售收入相关数据"""
        regions = {
            '美区': self.file_dir / f'{self.shop_name}_{self.month}_美国.xlsx',
            '欧区': self.file_dir / f'{self.shop_name}_{self.month}_欧区.xlsx',
            '全球': self.file_dir / f'{self.shop_name}_{self.month}_全球.xlsx'
        }

        total_order_amount = 0
        total_subsidy = 0
        total_refund = 0

        for region_name, file_path in regions.items():
            self.logger.info(f"\n正在处理 {region_name} 销售收入...")

            try:
                df = pd.read_excel(file_path, sheet_name='交易结算')

                order_amount = df[df['交易类型'] == '销售回款']['金额'].sum()
                subsidy = df[df['交易类型'] == '非商责补贴']['金额'].sum()
                refund = df[df['交易类型'] == '销售冲回']['金额'].sum()

                total_order_amount += order_amount
                total_subsidy += subsidy
                total_refund += refund

                self.logger.info(
                    f"  {region_name} - 订单金额: {order_amount:.2f}, 非商责补贴: {subsidy:.2f}, 平台退款: {refund:.2f}")

            except FileNotFoundError:
                self.logger.error(f"  错误：找不到文件 {file_path}")
            except Exception as e:
                self.logger.error(f"  错误：{e}")

        self.report['订单金额'] = round(total_order_amount, 2)
        self.report['非商责补贴'] = round(total_subsidy, 2)
        self.report['平台退款'] = round(total_refund, 2)
        self.report['冻结'] = self.frozen
        self.report['销售收入'] = round(total_order_amount + total_subsidy + total_refund + self.frozen, 2)

        self.logger.info(f"\n销售收入总计: {self.report['销售收入']:.2f}")

    async def calculate_order_cost(self):
        """计算订单成本"""
        self.logger.info("\n正在加载成本映射表...")
        self.single_sku_cost, self.combined_sku_cost = load_cost_mapping(use_cache=self.use_cache)

        if not self.single_sku_cost and not self.combined_sku_cost:
            self.logger.error("错误：未能加载任何成本映射数据")
            self.report['订单成本'] = 0
            return

        regions = {
            '美区': self.file_dir / f'{self.shop_name}_{self.month}_美国.xlsx',
            '欧区': self.file_dir / f'{self.shop_name}_{self.month}_欧区.xlsx',
            '全球': self.file_dir / f'{self.shop_name}_{self.month}_全球.xlsx'
        }

        total_cost = 0.0
        all_match_results = []  # 存储所有区域的匹配结果
        total_unmatched_count = 0  # 总未匹配数量

        for region_name, file_path in regions.items():
            self.logger.info(f"\n正在处理 {region_name} 订单成本...")

            try:
                region_cost, region_results, unmatched_skus = await self._calculate_region_order_cost(file_path,
                                                                                                      region_name)
                total_cost += region_cost
                all_match_results.extend(region_results)

                if unmatched_skus:
                    total_unmatched_count += len(unmatched_skus)

                self.logger.info(f"  {region_name} 订单成本: {region_cost:.2f}")

            except FileNotFoundError:
                self.logger.error(f"  错误：找不到文件 {file_path}")
            except Exception as e:
                self.logger.error(f"  错误：{e}")

        self.report['订单成本'] = round(total_cost, 2)
        self.logger.info(f"\n订单成本总计: {self.report['订单成本']:.2f}")

        # 如果有匹配失败，发送钉钉通知
        if total_unmatched_count > 0:
            try:
                message = f"📋 **成本匹配通知**\n\n店铺: {self.shop_name}\n月份: {self.month}\n匹配失败数量: {total_unmatched_count} 条"
                ding_bot_send("me", message)
                self.logger.info(f"  已发送钉钉通知，匹配失败数量: {total_unmatched_count}")
            except Exception as e:
                self.logger.error(f"  发送钉钉通知失败: {e}")

        # 导出匹配结果到Excel
        if all_match_results:
            try:
                result_df = pd.DataFrame(all_match_results)

                # 按区域分组统计
                summary_df = result_df.groupby(['区域', '匹配状态']).size().reset_index(name='数量')
                cost_summary = result_df[result_df['匹配状态'] == '成功'].groupby('区域')['行成本'].sum().reset_index(
                    name='区域成本')

                # 创建输出目录
                output_dir = self.file_dir / "match_results"
                output_dir.mkdir(parents=True, exist_ok=True)

                output_path = output_dir / f'{self.shop_name}_订单成本匹配结果_{self.month}.xlsx'

                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    # 详细结果
                    result_df.to_excel(writer, sheet_name='匹配详情', index=False)
                    # 汇总统计
                    summary_df.to_excel(writer, sheet_name='汇总统计', index=False)
                    # 区域成本汇总
                    cost_summary.to_excel(writer, sheet_name='区域成本', index=False)
                    # 只显示匹配失败的
                    failed_df = result_df[result_df['匹配状态'] == '失败']
                    if not failed_df.empty:
                        failed_df.to_excel(writer, sheet_name='匹配失败', index=False)

                self.logger.info(f"\n匹配结果已导出到: {output_path}")
                self.logger.info(f"  总记录数: {len(result_df)} 条")
                self.logger.info(f"  匹配成功: {len(result_df[result_df['匹配状态'] == '成功'])} 条")
                self.logger.info(f"  匹配失败: {len(result_df[result_df['匹配状态'] == '失败'])} 条")
                self.logger.info(f"  总成本: {result_df[result_df['匹配状态'] == '成功']['行成本'].sum():.2f}")
            except Exception as e:
                self.logger.error(f"导出匹配结果失败: {e}")

        return self.report['订单成本']

    async def _calculate_region_order_cost(self, file_path: Path, region_name: str) -> tuple:
        """计算单个区域的订单成本，并返回匹配结果"""
        df = pd.read_excel(file_path, sheet_name='交易结算')

        total_cost = 0.0
        matched_count = 0
        unmatched_skus = set()
        match_results = []

        for idx, row in df.iterrows():
            sku_attr = row['SKU货号']
            quantity = row['数量']

            if pd.isna(quantity):
                quantity = 0
            else:
                quantity = float(quantity)

            cost = get_cost_price(sku_attr, self.single_sku_cost, self.combined_sku_cost)

            if cost is not None and not pd.isna(cost):
                cost = float(cost)
                row_cost = cost * quantity
                total_cost += row_cost
                matched_count += 1

                match_results.append({
                    '区域': region_name,
                    'SKU': sku_attr,
                    '数量': quantity,
                    '成本价': cost,
                    '行成本': row_cost,
                    '匹配状态': '成功'
                })
            else:
                unmatched_skus.add(sku_attr)
                match_results.append({
                    '区域': region_name,
                    'SKU': sku_attr,
                    '数量': quantity,
                    '成本价': None,
                    '行成本': 0,
                    '匹配状态': '失败'
                })

        if unmatched_skus:
            unmatched_list = list(unmatched_skus)
            self.logger.info(f"  警告：以下SKU货号未匹配到成本价: {unmatched_list[:10]}")
            if len(unmatched_skus) > 10:
                self.logger.info(f"  ... 共 {len(unmatched_skus)} 个SKU未匹配")

        self.logger.info(f"  匹配成功 {matched_count} 条，未匹配 {len(unmatched_skus)} 条")

        return total_cost, match_results, unmatched_skus

    async def calculate_loss_package(self):
        """计算丢件成本"""
        manager = VerifyStockinManager(self.shop_name, self.job,self.month)
        total_diff_value = await manager.run()
        self.report['丢件'] = round(total_diff_value, 2)
        self.logger.info(f"\n丢件成本: {self.report['丢件']:.2f}")

    async def calculate_expenses(self):
        """计算平台各项费用"""
        seller_center_file = self.file_dir / f'{self.shop_name}_{self.month}_卖家中心.xlsx'

        self.logger.info(f"\n正在处理卖家中心表: {seller_center_file.name}")

        try:
            df = pd.read_excel(seller_center_file)

            # 平台罚款-售后
            after_sale_mask = df['备注'].str.contains('售后', na=False)
            after_sale_amount = df[after_sale_mask]['收支金额'].sum()
            self.report['平台罚款-售后'] = abs(round(after_sale_amount, 2))

            # 平台仓储费
            warehouse_mask = df['备注'] == '仓储综合服务费'
            warehouse_amount = df[warehouse_mask]['收支金额'].sum()
            self.report['平台仓储费'] = abs(round(warehouse_amount, 2))

            # EPR环保费
            epr_mask = df['备注'].str.contains('epr', case=False, na=False)
            epr_amount = df[epr_mask]['收支金额'].sum()
            self.report['EPR环保费'] = abs(round(epr_amount, 2))

            # 平台罚款
            penalty_mask = df['备注'].str.contains('商品品质保障', na=False)
            penalty_amount = df[penalty_mask]['收支金额'].sum()
            self.report['平台罚款'] = abs(round(penalty_amount, 2))

            # 广告服务费
            ad_mask = df['备注'] == '推广服务费'
            ad_amount = df[ad_mask]['收支金额'].sum()
            self.report['广告服务费'] = abs(round(ad_amount, 2))

            self.logger.info("\n平台费用计算结果：")
            self.logger.info(f"  平台罚款-售后: {self.report['平台罚款-售后']:.2f}")
            self.logger.info(f"  平台仓储费: {self.report['平台仓储费']:.2f}")
            self.logger.info(f"  EPR环保费: {self.report['EPR环保费']:.2f}")
            self.logger.info(f"  平台罚款: {self.report['平台罚款']:.2f}")
            self.logger.info(f"  广告服务费: {self.report['广告服务费']:.2f}")

        except FileNotFoundError:
            self.logger.error(f"错误：找不到文件 {seller_center_file}")
        except Exception as e:
            self.logger.error(f"错误：{e}")

    async def calculate_daily_bookkeeping(self, target_month=None):
        """计算日常记账费用（优化版）"""
        target_month=self.month
        # 查询数据
        daily_query = DingTalkDailyDataQuery(target_month=target_month)
        df = daily_query.load_data(force_refresh=False)

        if df.empty:
            self.logger.warning("没有日常记账数据")
            return {k: 0 for k in ['国内运费', '样品费', '服务费', '包装材料费', '单号费', '其他']}

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
            return {k: 0 for k in ['国内运费', '样品费', '服务费', '包装材料费', '单号费', '其他']}

        expense_summary = shop_df.groupby('支出项目')['金额'].sum().round(2)

        # 初始化费用并填充
        daily_map = {'国内运费': 0, '样品费': 0, '服务费': 0, '包装材料费': 0, '单号费': 0, '其他': 0}
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
        """计算毛利额、销售费用和经营利润"""
        # 销售成本 = 订单成本 + 丢件
        self.report['销售成本'] = round(self.report['订单成本'] + self.report['丢件'], 2)

        # 毛利额 = 销售收入 - 销售成本
        self.report['毛利额'] = round(self.report['销售收入'] - self.report['销售成本'], 2)

        # 销售费用 = 各项费用之和
        expense_fields = [
            '平台罚款-售后', '平台仓储费', 'EPR环保费', '平台罚款', '广告服务费',
            '国内运费', '样品费', '服务费', '包装材料费', '单号费', '其他'
        ]
        self.report['销售费用'] = round(sum(self.report[f] for f in expense_fields), 2)

        # 经营利润 = 毛利额 - 销售费用
        self.report['经营利润'] = round(self.report['毛利额'] - self.report['销售费用'], 2)

    async def generate_full_report(self):
        """生成完整财务报表"""
        print("=" * 60)
        self.logger.info(f"开始生成 {self.shop_name} {self.year} 财务报表")
        print("=" * 60)

        # 1. 计算销售收入
        await self.calculate_revenue()

        # 2. 计算订单成本
        await self.calculate_order_cost()

        # 3. 计算丢件成本
        await self.calculate_loss_package()

        # 4. 计算平台费用
        await self.calculate_expenses()

        # 5. 计算日常记账
        await self.calculate_daily_bookkeeping()

        # 6. 计算利润
        self.calculate_profit()

        print("\n" + "=" * 60)
        self.logger.info("财务报表生成完成")
        print("=" * 60)

        return self.report

    def get_report_dict(self):
        """返回报表字典"""
        return self.report


def export_all_to_excel(all_reports: list, shop_name_list: list, year: str, output_path: str = None):
    """导出所有店铺报表到Excel（指标作为行，店铺作为列）"""
    path = Path(
        __file__).resolve().parent.parent.parent.parent / "data" / "financial" / f"{year[4:6]}月份" / "full_report"
    path.mkdir(parents=True, exist_ok=True)
    if output_path is None:
        output_path = path / f"temu_财务报表_{year}.xlsx"

    # 定义指标顺序（第一列是指标名称）
    report_order = [
        '销售收入',
        '订单金额',
        '非商责补贴',
        '平台退款',
        '冻结',
        '销售成本',
        '订单成本',
        '丢件',
        '毛利额',
        '销售费用',
        '平台罚款-售后',
        '平台仓储费',
        'EPR环保费',
        '平台罚款',
        '广告服务费',
        '国内运费',
        '样品费',
        '服务费',
        '包装材料费',
        '单号费',
        '其他',
        '经营利润'
    ]

    # 构建数据：第一列是指标名称，后面每列是一个店铺的数据
    data = {'店铺': report_order}

    for i, shop_name in enumerate(shop_name_list):
        # 使用店铺名称作为列名
        column_name = shop_name
        data[column_name] = []
        for key in report_order:
            data[column_name].append(all_reports[i][key])

    # 创建DataFrame
    df = pd.DataFrame(data)
    df.to_excel(output_path, index=False)
    print(f"\n报表已导出到: {output_path}")
    return output_path


# 使用示例
async def main():
    shop_name_list = [
        "101-Temu全托管",
        "102-Temu全托管",
        "103-Temu全托管",
        "104-Temu全托管",
        "105-Temu全托管",
        "106-Temu全托管",
        "108-Temu全托管",
        "109-Temu全托管KA",
        "110-Temu全托管KA",
        "112-Temu全托管",
        "1101-Temu全托管",
        "1102-Temu全托管",
        "1103-Temu全托管",
        "1104-Temu全托管",
        "1105-Temu全托管",
        "1106-Temu全托管",
        "1107-Temu全托管",
        "1108-Temu全托管",
        "2101-Temu全托管KA",
        "2102-Temu全托管",
        "2103-Temu全托管",
        "2106-Temu全托管",
    ]

    shop_name_list=["102-Temu全托管","105-Temu全托管","1101-Temu全托管","2101-Temu全托管KA"]

    all_reports = []
    first_year = None

    for shop_name in shop_name_list:
        # 创建报表实例
        report = TemuFinancialReport(
            shop_name=shop_name,
            frozen=0,
            use_cache=True,
            job='financial_report',
            # year_month="202606"  # 可以指定日期，不指定就是上个月的数据
        )


        # 生成完整报表
        await report.generate_full_report()
        all_reports.append(report.get_report_dict())

        if first_year is None:
            first_year = report.year

    # 导出所有报表到同一个Excel
    if all_reports:
        export_all_to_excel(all_reports, shop_name_list, first_year)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
