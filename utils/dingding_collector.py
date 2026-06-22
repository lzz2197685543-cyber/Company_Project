# 这个目前主要获取钉钉中"temu-入库情况"的数据,然后读取筛选的数据

import csv
import os
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from utils.dingding_doc import DingTalkTokenManager, upload_multiple_records, test_delete_records, DingTalkSheetQuery
from calendar import monthrange


def get_prev_month_from_now():
    """
    返回当前时间的前一个月，返回两个值：(YYYY-MM, MM)
    """
    now = datetime.now()
    year = now.year
    month = now.month

    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1

    return f"{month:02d}"


def get_month_folder(month_str=None):
    """
    获取指定月份的数据文件夹路径

    Args:
        month_str: 月份字符串，格式如 '01', '02' 或 '2024-01'。如果为None，则使用上个月

    Returns:
        Path: 文件夹路径
    """
    if month_str is None:
        return Path(__file__).resolve().parent.parent.parent / "data" / f"{get_prev_month_from_now()}月份"

    # 处理不同的输入格式
    if '-' in month_str:
        # 格式如 '2024-01'
        year_month = month_str
        month = month_str.split('-')[1]
    else:
        # 格式如 '01'
        now = datetime.now()
        year = now.year
        month = int(month_str)
        # 如果指定的月份大于当前月份，说明是去年的数据
        if month > now.month:
            year -= 1
        year_month = f"{year}-{month:02d}"
        month = f"{month:02d}"

    return Path(__file__).resolve().parent.parent.parent / "data" / f"{month}月份"


class DingTalkDataCollector:
    """钉钉数据采集器，负责获取数据并保存到CSV"""

    def __init__(self, target_month=None):
        """
        初始化采集器

        Args:
            target_month: 目标月份，可以是：
                - None: 上个月（默认）
                - '01', '02' 等格式：当年指定月份
                - '2024-01' 格式：指定年份和月份
        """
        self.config = {
            "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
            "sheet_id": "BLtQjpE",  # 工作表ID
            "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
        }
        self.daily_config={
            "base_id": "14lgGw3P8vBvLGMEUQXEZEEL85daZ90D",
            "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE",
            "sheet_id": "9qzDNff"
        }

        self.target_month = target_month
        self.csv_dir = get_month_folder(target_month)

        # 创建缓存目录
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)

    def get_target_month_range(self, target_month=None):
        """
        获取指定月份的时间范围（毫秒时间戳）

        Args:
            target_month: 目标月份，格式同__init__

        Returns:
            tuple: (start_timestamp, end_timestamp, start_date, end_date)
        """
        if target_month is None:
            target_month = self.target_month

        today = datetime.now()

        if target_month is None:
            # 获取上个月
            first_day_of_this_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
            first_day_of_last_month = last_day_of_last_month.replace(day=1)

            start_date = first_day_of_last_month
            end_date = last_day_of_last_month
        else:
            # 解析年份和月份
            if isinstance(target_month, str):
                if '-' in target_month:
                    year, month = map(int, target_month.split('-'))
                else:
                    year = today.year
                    month = int(target_month)
                    # 如果指定的月份大于当前月份，说明是去年的数据
                    if month > today.month:
                        year -= 1
            else:
                raise ValueError(f"target_month必须是字符串，当前类型: {type(target_month)}")

            start_date = datetime(year, month, 1, 0, 0, 0)
            # 获取该月的最后一天
            last_day = monthrange(year, month)[1]
            end_date = datetime(year, month, last_day, 23, 59, 59)

        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)

        return start_timestamp, end_timestamp, start_date, end_date

    def fetch_and_save(self, force_refresh=False):
        """
        获取指定月份的数据并保存到CSV

        获取入库异常数据

        Args:
            force_refresh: 是否强制刷新（忽略已有缓存）

        Returns:
            str: CSV文件路径
        """
        # 生成缓存文件名（基于月份）
        start_timestamp, end_timestamp, start_date, end_date = self.get_target_month_range()
        filename = f"data_{start_date.strftime('%Y%m')}.csv"
        filepath = os.path.join(self.csv_dir, filename)

        # 如果文件已存在且不强制刷新，直接返回
        if os.path.exists(filepath) and not force_refresh:
            print(f"使用缓存文件: {filepath}")
            return filepath

        print(f"开始获取 {start_date.strftime('%Y-%m')} 月份的数据...")

        # 获取钉钉数据
        token_manager = DingTalkTokenManager()
        query = DingTalkSheetQuery(
            base_id=self.config["base_id"],
            sheet_id=self.config["sheet_id"],
            operator_id=self.config["operator_id"],
            token_manager=token_manager
        )

        all_records = query.get_all_records(batch_size=50)


        # 筛选指定月份的数据
        filtered_data = []
        for record in all_records:
            fields = record.get('fields', {})
            crawl_date = fields.get('收货时间')

            if crawl_date and start_timestamp <= crawl_date <= end_timestamp:
                # 处理店铺字段（统一格式）
                shop_field = fields.get('店铺')
                if isinstance(shop_field, dict):
                    fields['店铺名称'] = shop_field.get('name', '')
                    fields['店铺ID'] = shop_field.get('id', '')
                else:
                    fields['店铺名称'] = str(shop_field) if shop_field else ''
                    fields['店铺ID'] = ''

                # 处理平台字段
                platform_field = fields.get('平台')
                if isinstance(platform_field, dict):
                    fields['平台名称'] = platform_field.get('name', '')
                    fields['平台ID'] = platform_field.get('id', '')
                else:
                    fields['平台名称'] = str(platform_field) if platform_field else ''
                    fields['平台ID'] = ''

                filtered_data.append(fields)

        print(f"筛选出 {len(filtered_data)} 条记录")

        # 保存到CSV
        if filtered_data:
            self._save_to_csv(filtered_data, filepath)
        else:
            print("警告：没有筛选到任何数据")

        return filepath

    def fetch_and_save_daily(self, force_refresh=False):
        """
        获取每日入库数据（基于登记日期）并保存到CSV

        Args:
            force_refresh: 是否强制刷新（忽略已有缓存）

        Returns:
            str: CSV文件路径
        """
        # 生成缓存文件名（基于月份）
        start_timestamp, end_timestamp, start_date, end_date = self.get_target_month_range()
        filename = f"daily_data_{start_date.strftime('%Y%m')}.csv"
        filepath = os.path.join(self.csv_dir, filename)

        # 如果文件已存在且不强制刷新，直接返回
        if os.path.exists(filepath) and not force_refresh:
            print(f"使用缓存文件: {filepath}")
            return filepath

        print(f"开始获取 {start_date.strftime('%Y-%m')} 月份的每日入库数据...")

        # 获取钉钉数据
        token_manager = DingTalkTokenManager()
        query = DingTalkSheetQuery(
            base_id=self.daily_config["base_id"],
            sheet_id=self.daily_config["sheet_id"],
            operator_id=self.daily_config["operator_id"],
            token_manager=token_manager
        )

        all_records = query.get_all_records(batch_size=50)
        print(f"获取到 {len(all_records)} 条原始记录")

        # 筛选指定月份的数据（基于登记日期）
        filtered_data = []
        for record in all_records:
            fields = record.get('fields', {})
            # 使用登记日期字段进行筛选
            register_date = fields.get('登记日期')

            if register_date and start_timestamp <= register_date <= end_timestamp:
                # ... 处理逻辑保持不变 ...
                filtered_data.append(fields)

        print(f"筛选出 {len(filtered_data)} 条记录（{start_date.strftime('%Y-%m')}月份）")
        print(f"总金额: {sum(float(f.get('金额', 0)) for f in filtered_data if f.get('金额')):.2f}")

        # 保存到CSV（即使没有数据也创建空文件）
        self._save_to_csv(filtered_data, filepath)

        # 如果没有数据，创建一个空的CSV文件（只包含表头）
        if not filtered_data:
            print("警告：没有筛选到任何数据，创建空CSV文件")
            # 创建一个只有表头的空CSV文件
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                # 定义基本字段
                fieldnames = ['登记日期', '登记日期_格式化', '登记日期_年月日', '归属店铺', '店铺名称',
                              '店铺ID', '支出项目', '支出项目名称', '支出项目ID', '金额', '金额_数值',
                              '备注', '登记人', '登记人姓名', '登记人ID']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

        return filepath


    def _save_to_csv(self, data, filepath):
        """保存数据到CSV文件"""
        if not data:
            return

        # 获取所有字段名（合并所有记录的keys）
        fieldnames = set()
        for record in data:
            fieldnames.update(record.keys())

        # 排序字段名，方便查看
        fieldnames = sorted(list(fieldnames))

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"数据已保存到: {filepath}")
        print(f"共 {len(data)} 条记录，{len(fieldnames)} 个字段")


class DingTalkDataQuery:
    """从CSV文件中查询钉钉数据"""

    def __init__(self, target_month=None):
        """
        初始化查询器

        Args:
            target_month: 目标月份，可以是：
                - None: 上个月（默认）
                - '01', '02' 等格式：当年指定月份
                - '2024-01' 格式：指定年份和月份
        """
        self.target_month = target_month
        self.df = None
        self.csv_dir = get_month_folder(target_month)


    def get_target_month_range(self):
        """获取指定月份的时间范围"""
        collector = DingTalkDataCollector(self.target_month)
        return collector.get_target_month_range()

    def load_data(self):
        """加载CSV数据到DataFrame"""
        _, _, start_date, end_date = self.get_target_month_range()
        filename = f"data_{start_date.strftime('%Y%m')}.csv"
        csv_file_path = os.path.join(self.csv_dir, filename)

        # 检查文件是否存在
        if not os.path.exists(csv_file_path):
            print(f"警告：文件不存在 {csv_file_path}")
            print("请先运行采集器获取数据")
            return pd.DataFrame()

        if self.df is None:
            self.df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
            # 转换时间戳为日期格式（方便查看）
            if '数据爬取日期' in self.df.columns:
                self.df['数据爬取日期_格式化'] = pd.to_datetime(
                    self.df['数据爬取日期'],
                    unit='ms'
                )
            print(f"已加载 {len(self.df)} 条记录")
        return self.df

    def filter_by_shop(self, shop_name, exact_match=True):
        """
        根据店铺名称筛选

        Args:
            shop_name: 店铺名称
            exact_match: 是否精确匹配（False为模糊匹配）

        Returns:
            DataFrame: 筛选后的数据
        """
        df = self.load_data()

        if df.empty:
            print("没有数据可查询")
            return pd.DataFrame()

        if '店铺名称' not in df.columns:
            print("警告：CSV中没有'店铺名称'字段")
            return pd.DataFrame()

        if exact_match:
            result = df[df['店铺名称'] == shop_name]
        else:
            result = df[df['店铺名称'].str.contains(shop_name, na=False)]

        print(f"店铺 '{shop_name}' 筛选出 {len(result)} 条记录")
        return result

    def filter_by_platform(self, platform_name, exact_match=True):
        """根据平台名称筛选"""
        df = self.load_data()

        if df.empty:
            print("没有数据可查询")
            return pd.DataFrame()

        if '平台名称' not in df.columns:
            print("警告：CSV中没有'平台名称'字段")
            return pd.DataFrame()

        if exact_match:
            result = df[df['平台名称'] == platform_name]
        else:
            result = df[df['平台名称'].str.contains(platform_name, na=False)]

        print(f"平台 '{platform_name}' 筛选出 {len(result)} 条记录")
        return result

    def filter_by_date_range(self, start_date, end_date):
        """
        根据日期范围筛选

        Args:
            start_date: 开始日期 (datetime 或 字符串 'YYYY-MM-DD')
            end_date: 结束日期
        """
        df = self.load_data()

        if df.empty:
            print("没有数据可查询")
            return pd.DataFrame()

        if '数据爬取日期_格式化' not in df.columns:
            print("警告：没有日期字段")
            return pd.DataFrame()

        # 转换日期格式
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)

        result = df[(df['数据爬取日期_格式化'] >= start_date) &
                    (df['数据爬取日期_格式化'] <= end_date)]

        print(f"日期范围筛选出 {len(result)} 条记录")
        return result

    def get_statistics(self):
        """获取基本统计信息"""
        df = self.load_data()

        if df.empty:
            return {'总记录数': 0, '店铺列表': [], '平台列表': [], '日期范围': {'最早': None, '最晚': None}}

        stats = {
            '总记录数': len(df),
            '店铺列表': df['店铺名称'].unique().tolist() if '店铺名称' in df.columns else [],
            '平台列表': df['平台名称'].unique().tolist() if '平台名称' in df.columns else [],
            '日期范围': {
                '最早': df['数据爬取日期_格式化'].min() if '数据爬取日期_格式化' in df.columns else None,
                '最晚': df['数据爬取日期_格式化'].max() if '数据爬取日期_格式化' in df.columns else None
            }
        }

        return stats

    def export_to_excel(self, data, output_path):
        """导出筛选结果到Excel"""
        if isinstance(data, pd.DataFrame):
            data.to_excel(output_path, index=False, engine='openpyxl')
        else:
            pd.DataFrame(data).to_excel(output_path, index=False, engine='openpyxl')
        print(f"已导出到: {output_path}")


class DingTalkDailyDataQuery:
    """专门查询每日入库数据的类"""

    def __init__(self, target_month=None):
        """
        初始化每日数据查询器

        Args:
            target_month: 目标月份，可以是：
                - None: 上个月（默认）
                - '01', '02' 等格式：当年指定月份
                - '2024-01' 格式：指定年份和月份
        """
        self.target_month = target_month
        self.df = None
        self.csv_dir = get_month_folder(target_month)

    def load_data(self, force_refresh=False):
        """
        加载每日数据

        Args:
            force_refresh: 是否强制重新采集数据
        """
        # 先采集数据
        collector = DingTalkDataCollector(self.target_month)
        csv_file = collector.fetch_and_save_daily(force_refresh=force_refresh)

        # 检查文件是否存在
        if not os.path.exists(csv_file):
            print(f"警告：CSV文件不存在 {csv_file}")
            print("没有找到任何数据，返回空DataFrame")
            self.df = pd.DataFrame()  # 返回空DataFrame
            return self.df

        # 加载CSV文件
        try:
            self.df = pd.read_csv(csv_file, encoding='utf-8-sig')

            # 转换金额列为数值
            if '金额_数值' in self.df.columns:
                self.df['金额_数值'] = pd.to_numeric(self.df['金额_数值'], errors='coerce')
            elif '金额' in self.df.columns:
                self.df['金额_数值'] = pd.to_numeric(self.df['金额'], errors='coerce')

            print(f"已加载 {len(self.df)} 条记录")
            return self.df
        except Exception as e:
            print(f"加载CSV文件失败: {e}")
            self.df = pd.DataFrame()
            return self.df

    def filter_by_shop(self, shop_name, exact_match=True):
        """
        根据店铺名称筛选

        Args:
            shop_name: 店铺名称
            exact_match: 是否精确匹配（False为模糊匹配）

        Returns:
            DataFrame: 筛选后的数据
        """
        df = self.load_data()

        if df.empty:
            print("没有数据可查询")
            return pd.DataFrame()

        if '店铺名称' not in df.columns:
            print("警告：CSV中没有'店铺名称'字段")
            return pd.DataFrame()

        if exact_match:
            result = df[df['店铺名称'] == shop_name]
        else:
            result = df[df['店铺名称'].str.contains(shop_name, na=False)]

        print(f"店铺 '{shop_name}' 筛选出 {len(result)} 条记录")
        if not result.empty:
            print(f"总金额: {result['金额_数值'].sum():.2f}")

        return result

    def filter_by_expense_item(self, expense_name, exact_match=True):
        """
        根据支出项目筛选

        Args:
            expense_name: 支出项目名称
            exact_match: 是否精确匹配
        """
        df = self.load_data()

        if df.empty:
            print("没有数据可查询")
            return pd.DataFrame()

        if '支出项目名称' not in df.columns:
            print("警告：CSV中没有'支出项目名称'字段")
            return pd.DataFrame()

        if exact_match:
            result = df[df['支出项目名称'] == expense_name]
        else:
            result = df[df['支出项目名称'].str.contains(expense_name, na=False)]

        print(f"支出项目 '{expense_name}' 筛选出 {len(result)} 条记录")
        if not result.empty:
            print(f"总金额: {result['金额_数值'].sum():.2f}")

        return result

    def filter_by_registrant(self, registrant_name, exact_match=True):
        """
        根据登记人筛选

        Args:
            registrant_name: 登记人姓名
            exact_match: 是否精确匹配
        """
        df = self.load_data()

        if df.empty:
            print("没有数据可查询")
            return pd.DataFrame()

        if '登记人姓名' not in df.columns:
            print("警告：CSV中没有'登记人姓名'字段")
            return pd.DataFrame()

        if exact_match:
            result = df[df['登记人姓名'] == registrant_name]
        else:
            result = df[df['登记人姓名'].str.contains(registrant_name, na=False)]

        print(f"登记人 '{registrant_name}' 筛选出 {len(result)} 条记录")
        if not result.empty:
            print(f"总金额: {result['金额_数值'].sum():.2f}")

        return result

    def filter_by_date_range(self, start_date, end_date):
        """
        根据登记日期范围筛选

        Args:
            start_date: 开始日期 (datetime 或 字符串 'YYYY-MM-DD')
            end_date: 结束日期
        """
        df = self.load_data()

        if df.empty:
            print("没有数据可查询")
            return pd.DataFrame()

        if '登记日期_格式化' not in df.columns:
            print("警告：没有日期字段")
            return pd.DataFrame()

        # 转换日期格式
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)

        result = df[(df['登记日期_格式化'] >= start_date) &
                    (df['登记日期_格式化'] <= end_date)]

        print(f"日期范围筛选出 {len(result)} 条记录")
        if not result.empty:
            print(f"总金额: {result['金额_数值'].sum():.2f}")

        return result

    def filter_by_remark_keyword(self, keyword):
        """
        根据备注关键词筛选

        Args:
            keyword: 关键词
        """
        df = self.load_data()

        if df.empty:
            print("没有数据可查询")
            return pd.DataFrame()

        if '备注' not in df.columns:
            print("警告：CSV中没有'备注'字段")
            return pd.DataFrame()

        result = df[df['备注'].str.contains(keyword, na=False)]

        print(f"备注包含 '{keyword}' 筛选出 {len(result)} 条记录")
        if not result.empty:
            print(f"总金额: {result['金额_数值'].sum():.2f}")

        return result

    def get_shop_summary(self):
        """获取按店铺汇总的统计信息"""
        df = self.load_data()

        if df.empty:
            return pd.DataFrame()

        summary = df.groupby('店铺名称').agg({
            '金额_数值': ['sum', 'mean', 'count'],
            '备注': 'count'
        }).round(2)

        summary.columns = ['总金额', '平均金额', '记录数']
        summary = summary.sort_values('总金额', ascending=False)

        print("\n=== 店铺汇总统计 ===")
        print(summary)
        print(f"\n总计: {df['金额_数值'].sum():.2f}")

        return summary

    def get_expense_summary(self):
        """获取按支出项目汇总的统计信息"""
        df = self.load_data()

        if df.empty:
            return pd.DataFrame()

        summary = df.groupby('支出项目名称').agg({
            '金额_数值': ['sum', 'mean', 'count']
        }).round(2)

        summary.columns = ['总金额', '平均金额', '记录数']
        summary = summary.sort_values('总金额', ascending=False)

        print("\n=== 支出项目汇总统计 ===")
        print(summary)

        return summary

    def export_to_excel(self, data, output_path):
        """导出筛选结果到Excel"""
        if isinstance(data, pd.DataFrame):
            data.to_excel(output_path, index=False, engine='openpyxl')
        else:
            pd.DataFrame(data).to_excel(output_path, index=False, engine='openpyxl')
        print(f"已导出到: {output_path}")


def query_data_for_month(month=None, shop_name=None, platform_name=None):
    """
    便捷函数：查询指定月份的数据

    Args:
        month: 月份，如 '01', '02' 或 '2024-01'
        shop_name: 店铺名称（可选）
        platform_name: 平台名称（可选）

    Returns:
        DataFrame: 查询结果
    """
    # 先采集数据
    collector = DingTalkDataCollector(month)
    collector.fetch_and_save(force_refresh=False)

    # 再查询
    query = DingTalkDataQuery(month)

    if shop_name:
        return query.filter_by_shop(shop_name, exact_match=False)
    elif platform_name:
        return query.filter_by_platform(platform_name, exact_match=False)
    else:
        return query.load_data()


if __name__ == '__main__':
    # ========== 示例1：查询上个月的数据（默认） ==========
    # collector = DingTalkDataCollector()  # 不指定月份，默认上个月
    # csv_file = collector.fetch_and_save(force_refresh=False)
    #
    # query = DingTalkDataQuery()  # 不指定月份，默认上个月
    #
    # # 查看统计信息
    # stats = query.get_statistics()
    # print(f"统计信息: {stats}")
    #
    # # 筛选特定店铺的数据
    # shop_data = query.filter_by_shop("105-Temu全托管", exact_match=True)
    # print(shop_data.head())

    # ========== 示例2：查询指定月份（如1月份） ==========
    # 方式1：使用月份数字
    collector_jan0 = DingTalkDataCollector(target_month="05")
    csv_file_jan0 = collector_jan0.fetch_and_save(force_refresh=False)

    query_jan = DingTalkDataQuery(target_month="05")
    stats_jan = query_jan.get_statistics()
    print(f"4月份统计信息: {stats_jan}")

    # collector_jan = DingTalkDataCollector(target_month="06")
    # csv_file_jan = collector_jan.fetch_and_save_daily(force_refresh=False)

    # 方式1：直接使用 DingTalkDailyDataQuery
    # daily_query = DingTalkDailyDataQuery(target_month="06")
    # daily_data = daily_query.load_data(force_refresh=False)
    #
    # # 查看数据概况
    # print(f"\n数据列: {daily_data.columns.tolist()}")
    #
    # # 筛选特定店铺
    # shop_data = daily_query.filter_by_shop("线下-未分摊", exact_match=True)
    # print(shop_data[['店铺名称', '支出项目名称', '金额', '备注', '登记人姓名']].head())


    # 导出到Excel
    # query.export_to_excel(filtered, "output.xlsx")