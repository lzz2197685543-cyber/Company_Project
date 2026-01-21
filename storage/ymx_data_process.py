import pandas as pd
from datetime import datetime
import os
import pymysql
from pathlib import Path
from utils.logger import get_logger
CONFIG_DIR = Path(__file__).resolve().parent.parent / "data"


# ---------------- 数据库配置 ----------------
HOST = "localhost"
USER = "root"
PASSWORD = "1234"
DB = "py_spider"
PORT = 3306

class DataProcessor:
    """Temu数据处理类 - 精简版"""

    def __init__(self, data_dir=CONFIG_DIR):
        """
        初始化处理器
        Args:
            data_dir: 数据目录路径
        """
        self.data_dir = data_dir
        self.logger = get_logger('YmxNews')
        self.current_date = datetime.now().strftime("%Y%m%d")
        # self.upload_path = os.path.join(data_dir, 'upload.csv')
        self.keys = ['商品ID']

        self.db_conf = dict(
            host="localhost",
            user="root",
            password="1234",
            database="py_spider",
            charset="utf8mb4"
        )

        # 确保目录存在
        os.makedirs(data_dir, exist_ok=True)

    def get_upload_data(self, platform="亚马逊") -> pd.DataFrame:
        """
        从 product_monitor 中读取指定平台的数据
        """
        conn = pymysql.connect(**self.db_conf)

        try:
            sql = """
            SELECT
                discover_date,
                source_platform,
                product_id AS 商品ID,
                image_url AS 图片,
                product_name AS 产品名称,
                launch_date AS 上架日期,
                total_sales AS 总销量,
                monthly_sales AS 月销量,
                managed_mode AS 托管模式,
                selling_site AS 在售站点,
                product_url AS 产品链接,
                category AS 类目
            FROM product_monitor
            WHERE source_platform = %s
            """

            df = pd.read_sql(sql, conn, params=[platform])
            return df

        finally:
            conn.close()

    def filter_new_data(self):
        """核心功能：筛选新数据"""
        try:
            # 1. 读取本周数据
            df_filename = os.path.join(self.data_dir, f"ymx_get_new_{self.current_date}.csv")
            if not os.path.exists(df_filename):
                raise FileNotFoundError(f"本周数据文件不存在：{df_filename}")

            df = pd.read_csv(df_filename)

            # 2. 按商品ID去重，保留销量最高的
            df_result = df.sort_values('月销量', ascending=False).drop_duplicates('商品ID', keep='first')

            # 3. 读取历史数据
            df_last_upload = self.get_upload_data()

            # # 4. 备份历史数据
            # backup_filename = os.path.join(self.data_dir, f'upload_{self.current_date}_backup.csv')
            # df_last_upload.to_csv(backup_filename, index=False, encoding='utf-8-sig')

            # 5. 筛选本周新出现的商品
            # 统一数据类型为字符串
            df_result[self.keys] = df_result[self.keys].fillna('').astype(str)
            df_last_upload[self.keys] = df_last_upload[self.keys].fillna('').astype(str)

            # 找出本周有但上周没有的商品
            existing_ids = df_last_upload[self.keys].drop_duplicates()
            df_new = df_result.merge(existing_ids, on=self.keys, how='left', indicator=True)
            df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge'])

            # 6. 保存新数据（覆盖旧数据）
            # df_new.to_csv(self.upload_path, index=False, encoding='utf-8-sig')
            #
            self.logger.info(f"处理完成: 本周新增 {len(df_new)} 条记录")
            self.logger.info(f"原始数据: {len(df)} 条")
            self.logger.info(f"去重后: {len(df_result)} 条")

            return df_new

        except Exception as e:
            self.logger.error(f"数据处理错误: {str(e)}")
            return pd.DataFrame()

    def build_records(self,df_new):
        """构建上传记录"""
        try:

            temu_df = df_new

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
                    "月销量": row['月销量'],
                    "在售站点": row['在售站点'],
                    "类目": row['类目'],
                }
                records.append(record)

            return records

        except Exception as e:
            self.logger.info(f"构建记录错误: {str(e)}")
            return []

    def parse_date(self,x):
        if pd.isnull(x):
            return None
        try:
            # 尝试按数字时间戳处理（毫秒）
            return datetime.fromtimestamp(int(x) / 1000).date()
        except (ValueError, TypeError):
            try:
                # 尝试按 ISO 字符串处理
                # 去掉时区信息 +00:00，直接解析
                if '+' in str(x):
                    x = str(x).split('+')[0]
                return pd.to_datetime(x).date()
            except Exception:
                # 都失败返回 None
                return None

    def safe_value(self,x, default=None):
        """把 NaN 转换成 MySQL 可用的 None，或提供默认值"""
        if pd.isnull(x):
            return default
        return x

    def import_csv_to_product_monitor(self,df_new):
        """
        将 CSV 数据导入 product_monitor 表
        :param csv_path: CSV 文件路径
        """

        # 读取 CSV
        df = df_new
        df['上架日期'] = df['上架日期'].apply(self.parse_date)
        df['发现日期'] = df['发现日期'].apply(self.parse_date)

        connection = pymysql.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB,
            port=PORT,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with connection.cursor() as cursor:
                insert_sql = """
                INSERT INTO product_monitor (
                    discover_date, source_platform, product_id, image_url, product_name,
                    launch_date, total_sales, monthly_sales, managed_mode, selling_site,
                    product_url, category
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                for _, row in df.iterrows():
                    cursor.execute(insert_sql, (
                        self.safe_value(row['发现日期']),
                        self.safe_value(row['来源平台']),
                        self.safe_value(row['商品ID']),
                        self.safe_value(row['图片']),
                        str(self.safe_value(row['产品名称'])),  # 截断，防止太长
                        self.safe_value(row['上架日期']),
                        int(self.safe_value(row.get('总销量'), 0)) , # CSV 没有总销量就用 0,  # 默认 0
                        int(self.safe_value(row.get('月销量'), 0)),  # 默认 0
                        self.safe_value(row.get('托管模式')),  # ✅ 关键补位
                        self.safe_value(row['在售站点']),
                        self.safe_value(row['产品链接']),
                        self.safe_value(row['类目'])
                    ))

                connection.commit()
                print(f"已成功插入 {len(df)} 条数据到 product_monitor 表！")
        finally:
            connection.close()


# 使用示例
# if __name__ == "__main__":
#     # # 创建处理器
#     processor = DataProcessor()
#
#     # 筛选新数据
#     new_data = processor.filter_new_data()
#
#     # 构建上传记录
#     if not new_data.empty:
#         records = processor.build_records(new_data)
#         print(f"构建了 {len(records)} 条上传记录")
#
#         # 预览前3条
#         for i, record in enumerate(records[:3]):
#             print(f"记录{i + 1}: ID={record['商品ID']}, 名称={record['产品名称'][:30]}...")
#     else:
#         print("没有新数据需要上传")
