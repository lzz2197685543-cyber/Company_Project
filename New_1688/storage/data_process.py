import os
import re
import pandas as pd
from datetime import datetime


class DataProcessor:
    def __init__(self, current_date=None):
        self.data_dir = os.path.join(os.getcwd(), 'data')  # 这里直接使用绝对路径
        self.current_date = current_date or datetime.now().strftime("%Y-%m-%d")
        self.current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 获取当前时间，用于“发现日期”

        self.upload_path = os.path.join(self.data_dir, 'upload.csv')
        self.keys = ['商品ID']

    def find_matching_file(self):
        """查找符合条件的文件"""
        # 正则表达式，匹配类似的文件名
        pattern = re.compile(rf"店雷达_1688选品库-近30天_{self.current_date}_\d{{6}}\.xls")

        # 遍历文件夹，找到符合匹配的文件
        for filename in os.listdir(self.data_dir):
            if pattern.match(filename):
                file_path = os.path.join(self.data_dir, filename)
                print(f"找到匹配的文件: {filename}")
                return file_path
        print("没有找到匹配的文件")
        return None

    def process_data(self, file_path):
        """处理文件数据"""
        # 读取文件
        df = pd.read_excel(file_path)

        # 选择需要的列并创建副本
        columns_needed = ['商品类目', '商品标题', '商品ID', '商品链接(点击下方链接可跳转)', '上架时间', '所在地', '总件数', '主图链接(点击下方链接可跳转)']
        df_selected = df[columns_needed].copy()  # 创建副本

        # 重命名列
        df_selected.columns = ['类目', '产品名称', '商品ID', '产品链接', '上架日期', '在售站点', '总销量', '图片']

        # 使用 .loc 来添加“发现日期”和“来源”列
        df_selected.loc[:, '发现日期'] = self.current_time
        df_selected.loc[:, '来源平台'] = '1688'

        # 将类目列以 ">" 分割，并取第二个元素（索引从 0 开始）
        df_selected['类目'] = df_selected['类目'].str.split('>').str[1]

        # 返回处理后的数据
        return df_selected


    def filter_new_data(self,df):
        """核心功能：筛选新数据"""
        try:
            # 2. 按商品ID去重，保留销量最高的
            df_result = df.sort_values('总销量', ascending=False).drop_duplicates('商品ID', keep='first')

            # 3. 读取历史数据
            df_last_upload = pd.read_csv(self.upload_path) if os.path.exists(self.upload_path) \
                else pd.DataFrame(columns=self.keys)

            # 4. 备份历史数据
            backup_filename = os.path.join(self.data_dir, f'upload_{self.current_date}_backup.csv')
            df_last_upload.to_csv(backup_filename, index=False, encoding='utf-8-sig')

            # 5. 筛选本周新出现的商品
            # 统一数据类型为字符串
            df_result[self.keys] = df_result[self.keys].fillna('').astype(str)
            df_last_upload[self.keys] = df_last_upload[self.keys].fillna('').astype(str)

            # 找出本周有但上周没有的商品
            existing_ids = df_last_upload[self.keys].drop_duplicates()
            df_new = df_result.merge(existing_ids, on=self.keys, how='left', indicator=True)
            df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge'])

            # 6. 保存新数据（覆盖旧数据）
            df_new.to_csv(self.upload_path, index=False, encoding='utf-8-sig')
            print(f"原始数据: {len(df)} 条")
            print(f"去重后: {len(df_result)} 条")
            print(f"处理完成: 本周新增 {len(df_new)} 条记录")

            return df_new

        except Exception as e:
            print(f"数据处理错误: {str(e)}")
            return pd.DataFrame()

    def build_records(self):
        """构建上传记录"""
        try:
            if not os.path.exists(self.upload_path):
                return []

            temu_df = pd.read_csv(self.upload_path)

            if temu_df.empty:
                return []

            # 构建记录列表
            records = []
            for _, row in temu_df.iterrows():
                record = {
                    "发现日期": row['发现日期'],
                    "来源平台": row['来源平台'],
                    "商品ID": row["商品ID"],
                    "图片": {"text": row['图片'], "link": row['图片']},
                    "产品名称": row['产品名称'],
                    "产品链接": {"text": row['产品链接'], "link": row['产品链接']},
                    "上架日期": row["上架日期"],
                    "总销量": row['总销量'],
                    "在售站点": row['在售站点'],
                    "类目": row['类目'],
                }
                records.append(record)

            return records

        except Exception as e:
            print(f"构建记录错误: {str(e)}")
            return []



    def execute(self):
        """执行完整的数据处理过程"""
        # 查找匹配的文件
        file_path = self.find_matching_file()
        if file_path:
            # 处理数据 作为本周的数据
            df_processed = self.process_data(file_path)

            # 我们要与上周上传的数据进行对比，确定这周要上传的数据
            self.filter_new_data(df_processed)

            # 构建要上传的数据格式
            recoders=self.build_records()
            return recoders

        return None

if __name__ == '__main__':
    d=DataProcessor('../data')
    d.execute()