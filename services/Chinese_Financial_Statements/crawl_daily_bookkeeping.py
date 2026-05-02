import asyncio
import csv
from datetime import datetime,timedelta
from core.base_client import BaseClient
import os
from pathlib import Path
import pandas as pd


DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "daily_bookkeeping"
DATA_DIR.mkdir(exist_ok=True)

class DailyBookkeeping(BaseClient):
    def __init__(self, job, target_shops,start_date=None, end_date=None):
        super().__init__(job)
        self.start_date = start_date
        self.end_date = end_date
        self.target_shops = target_shops

    def get_previous_month_range(self, date_format: str = "%Y-%m-%d") -> tuple:
        """获取当前时间前一个月的整月时间范围（结束日期+1天）"""
        today = datetime.now()
        first_day_of_current_month = today.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

        start_date = first_day_of_previous_month.strftime(date_format)
        end_date = last_day_of_previous_month.strftime(date_format)
        return start_date, end_date

    async def get_info(self, page):
        """获取费用流水数据"""
        # 获取时间范围
        if self.start_date and self.end_date:
            start_date = self.start_date
            end_date = self.end_date
        else:
            start_date, end_date = self.get_previous_month_range()

        print(f"开始获取费用流水数据: {start_date} ~ {end_date}")


        url = 'https://pf.erp321.com/WebApi/PF/FeeFlowing/GetFeeFlowingListWidthFld'
        params = {
            'uvalue': 'pfcweb_process.daily-book',
        }

        json_data = {
            'ip': '',
            'page': {
                'currentPage': page,
                'pageSize': 500,  # 增大pageSize以获取更多数据
            },
            'uid': '',
            'coid': '',
            'data': {
                'dateType': 1,
                'statusList': ['Confirmed'],
                'csgIdList': [],
                'shopIdList': [],
                'beginDate': start_date,
                'endDate': end_date,
            },
            'ssProjectType': 'SS',
            'ssClientType': 'Web',
        }

        data = await self.post_sync(url=url, params=params, payload=json_data)
        return data

    def parse(self, data):
        """解析数据并返回列表格式"""
        if 'data' in data and 'dataList' in data['data']:
            data_list = data['data']['dataList']
        else:
            data_list = []

        parsed_rows = []
        for item in data_list:
            row = {
                '编号': item.get('ffId', ''),
                '类型': item.get('flowTypeName', ''),
                '费用类型': item.get('feeTypeTxt', ''),
                '是否分摊': item.get('isSetSplitTxt', ''),
                '发生日期': item.get('ffDate', ''),
                '店铺/其他成本中心': item.get('shopName', ''),
                '项目名称': item.get('csgName', ''),
                '账户': item.get('accountName', ''),
                '状态': item.get('statusName', ''),
                '摘要': item.get('remark', ''),
                '方向': item.get('directorName', ''),
                '金额': item.get('amount', 0),
                '美元金额': item.get('usdAmount', 0),
                '列示方式': item.get('showTypeName', ''),
                '列示区间': f"{item.get('showBeginDate', '')} ~ {item.get('showEndDate', '')}" if item.get(
                    'showBeginDate') or item.get('showEndDate') else '',
                '关联云会计状态': item.get('cloudLinkStatus', ''),
                '关联云会计凭证字号': item.get('cloudLinkVoucherNo', ''),
                '关联云会计账套': item.get('cloudLinkAccountCode', ''),
                '分摊类型': item.get('splitTypeTxt', ''),
                '分摊方式': item.get('splitMethodTxt', ''),
                '分摊到指定成本中心': '',
                '分摊计算公式': '',
                '是否排除分类单': item.get('excludeSpecialTxt', ''),
                '分摊范围': '',
                '指定店铺/店铺组/平台': str(item.get('shopGroupList', '')),
                '固定金额/比例': '',
                '根据发货仓分摊': '',
                '调整分摊基数': '',
                '指定分摊金额': '',
                '登记人': item.get('creatorName', ''),
                '登记时间': item.get('created', ''),
                '编辑人': item.get('modifierName', ''),
                '编辑时间': item.get('modified', ''),
                '美元汇率': item.get('currencyRmbUsd', ''),
                '是否分摊到订单': item.get('splitToOrder', False),
                '是否过滤无订单的日期': '',
            }
            parsed_rows.append(row)

        return parsed_rows

    def save(self,items):
        if not items:
            return

        current_date = datetime.now().strftime("%Y%m%d")
        fname = f"{DATA_DIR}/daily_bookkeeping_{current_date}.csv"
        exists = os.path.exists(fname)

        with open(fname, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=items[0].keys())
            if not exists:
                writer.writeheader()
            writer.writerows(items)

    async def get_all_page(self):
        """主函数"""
        page=1
        all_items=[]
        while True:
            self.logger.info(f'正在爬取第{page}页的数据')

            # 获取数据
            data = await self.get_info(page)

            # 解析数据
            parsed_rows = self.parse(data)

            all_items.extend(parsed_rows)



            if len(parsed_rows)<500:
                self.logger.info("没有下一页了")
                break

            page += 1


        # 保存数据
        # self.save(all_items)
        self.logger.info(f"共获取 {len(all_items)} 条记录")
        return all_items

    async def get_money(self):
        """按项目名称和店铺分类汇总金额"""
        # 获取所有数据
        all_items = await self.get_all_page()

        # 转换为DataFrame
        df = pd.DataFrame(all_items)

        # 定义需要统计的项目名称（完整字符串）
        target_projects = [
            'F15110001 广告推广费',
            'F15110002 国内运费',
            'F15110003 软件费用',
            'F15110007 样品费',
            'F15110013 包装材料费',
            'F15110023 单号费',
            'F15110099 其他'
        ]

        # 筛选出指定项目和指定店铺的费用记录
        filtered_df = df[
            (df['项目名称'].isin(target_projects)) &
            (df['店铺/其他成本中心'].isin(self.target_shops))
            ]

        # 如果金额字段是字符串类型，转换为数值类型
        if not filtered_df.empty and filtered_df['金额'].dtype == 'object':
            filtered_df['金额'] = pd.to_numeric(filtered_df['金额'], errors='coerce')

        # 先创建一个空的DataFrame，包含所有店铺和项目的组合，初始金额为0
        # 生成所有店铺×项目的组合
        index = pd.MultiIndex.from_product([self.target_shops, target_projects],
                                           names=['店铺/其他成本中心', '项目名称'])
        empty_df = pd.DataFrame(index=index).reset_index()
        empty_df['金额'] = 0

        if filtered_df.empty:
            print("未找到符合条件的费用记录，返回全0表格")
            summary = empty_df.pivot_table(
                index='店铺/其他成本中心',
                columns='项目名称',
                values='金额',
                fill_value=0
            )
            # 确保列顺序与target_projects一致
            summary = summary[target_projects]
            print(summary)
            return summary, empty_df

        # 分组汇总
        grouped = filtered_df.groupby(['店铺/其他成本中心', '项目名称'])['金额'].sum().reset_index()

        # 将汇总结果与空表格合并，缺失的自动补0
        merged = empty_df.merge(grouped, on=['店铺/其他成本中心', '项目名称'],
                                suffixes=('_default', '_actual'), how='left')
        merged['金额'] = merged['金额_actual'].fillna(merged['金额_default'])
        merged = merged[['店铺/其他成本中心', '项目名称', '金额']]

        # 透视成表格形式
        summary = merged.pivot_table(
            index='店铺/其他成本中心',
            columns='项目名称',
            values='金额',
            fill_value=0
        )

        # 确保列顺序与target_projects一致
        summary = summary[target_projects]

        print(summary)

        return summary, filtered_df


if __name__ == '__main__':
    d = DailyBookkeeping("chinese_financial_statements")
    asyncio.run(d.get_info('12'))
