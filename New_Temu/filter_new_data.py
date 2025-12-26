import pandas as pd
from datetime import datetime
import os
from logger_config import SimpleLogger


class TemuDataProcessor:
    """Temu数据处理类 - 精简版"""

    def __init__(self, data_dir='./data/data'):
        """
        初始化处理器
        Args:
            data_dir: 数据目录路径
        """
        self.logger = SimpleLogger('filter_new_data')
        self.data_dir = data_dir
        self.current_date = datetime.now().strftime("%Y%m%d")
        self.upload_path = os.path.join(data_dir, 'upload.csv')
        self.keys = ['商品ID']

        # 确保目录存在
        os.makedirs(data_dir, exist_ok=True)

    def filter_new_data(self):
        """核心功能：筛选新数据"""
        try:
            # 1. 读取本周数据
            df_filename = os.path.join(self.data_dir, f"temu_get_new_{self.current_date}.csv")
            if not os.path.exists(df_filename):
                raise FileNotFoundError(f"本周数据文件不存在：{df_filename}")

            df = pd.read_csv(df_filename)

            # 2. 筛选月销量≥300的数据
            df_filtered = df[df['月销量'] >= 300]

            # 3. 按商品ID去重，保留销量最高的
            df_result = df_filtered.sort_values('月销量', ascending=False).drop_duplicates('商品ID', keep='first')

            # 4. 读取历史数据
            df_last_upload = pd.read_csv(self.upload_path) if os.path.exists(self.upload_path) \
                else pd.DataFrame(columns=self.keys)

            # 5. 备份历史数据
            backup_filename = os.path.join(self.data_dir, f'upload_{self.current_date}_backup.csv')
            df_last_upload.to_csv(backup_filename, index=False, encoding='utf-8-sig')

            # 6. 筛选本周新出现的商品
            # 统一数据类型为字符串
            df_result[self.keys] = df_result[self.keys].fillna('').astype(str)
            df_last_upload[self.keys] = df_last_upload[self.keys].fillna('').astype(str)

            # 找出本周有但上周没有的商品
            existing_ids = df_last_upload[self.keys].drop_duplicates()
            df_new = df_result.merge(existing_ids, on=self.keys, how='left', indicator=True)
            df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge'])

            # 7. 保存新数据（覆盖旧数据）
            df_new.to_csv(self.upload_path, index=False, encoding='utf-8-sig')

            self.logger.info(f"处理完成: 本周新增 {len(df_new)} 条记录")
            self.logger.info(f"原始数据: {len(df)} 条")
            self.logger.info(f"筛选后(月销量≥300): {len(df_filtered)} 条")
            self.logger.info(f"去重后: {len(df_result)} 条")

            return df_new

        except Exception as e:
            self.logger.error(f"数据处理错误: {str(e)}")
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
                    "月销量": row['月销量'],
                    "托管模式": row['托管模式'],
                    "在售站点": row['在售站点'],
                    "类目": row['类目'],
                }
                records.append(record)

            return records

        except Exception as e:
            self.logger.info(f"构建记录错误: {str(e)}")
            return []


# 使用示例
if __name__ == "__main__":
    # 创建处理器
    processor = TemuDataProcessor()

    # 筛选新数据
    new_data = processor.filter_new_data()

    # 构建上传记录
    if not new_data.empty:
        records = processor.build_records()
        print(f"构建了 {len(records)} 条上传记录")

        # 预览前3条
        for i, record in enumerate(records[:3]):
            print(f"记录{i + 1}: ID={record['商品ID']}, 名称={record['产品名称'][:30]}...")
    else:
        print("没有新数据需要上传")