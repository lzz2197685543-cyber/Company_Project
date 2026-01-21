import pymysql
import pandas as pd
from datetime import datetime
from pathlib import Path

# ---------------- 数据库配置 ----------------
HOST = "localhost"
USER = "root"
PASSWORD = "1234"
DB = "py_spider"
PORT = 3306

def parse_date(x):
    """兼容时间戳（毫秒）和 ISO 字符串，返回 YYYY-MM-DD 或 None"""
    if pd.isnull(x):
        return None
    try:
        # 毫秒时间戳
        return datetime.fromtimestamp(int(x) / 1000).date()
    except (ValueError, TypeError):
        try:
            # ISO 字符串，去掉时区 +00:00
            x_str = str(x)
            if '+' in x_str:
                x_str = x_str.split('+')[0]
            return pd.to_datetime(x_str).date()
        except Exception:
            return None

def safe_value(x, default=None):
    """把 NaN 转换成 MySQL 可用的 None 或默认值"""
    if pd.isnull(x):
        return default
    return x

def safe_int(row, col):
    """获取整数字段，如果缺失或空值，返回 0"""
    if col in row and pd.notnull(row[col]):
        try:
            return int(row[col])
        except:
            return 0
    return 0

def create_product_monitor_table():
    """创建 product_monitor 表"""
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
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS product_monitor (
                id INT PRIMARY KEY AUTO_INCREMENT,
                discover_date DATE COMMENT '发现日期',
                source_platform VARCHAR(50) COMMENT '来源平台',
                product_id VARCHAR(100) COMMENT '商品ID',
                image_url TEXT COMMENT '商品图片',
                product_name TEXT COMMENT '产品名称',
                launch_date DATE COMMENT '上架日期',
                total_sales INT DEFAULT 0 COMMENT '总销量',
                monthly_sales INT DEFAULT 0 COMMENT '月销量',
                managed_mode VARCHAR(50) COMMENT '托管模式',
                selling_site VARCHAR(100) COMMENT '在售站点',
                product_url TEXT COMMENT '产品链接',
                category VARCHAR(100) COMMENT '类目',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产品监控表';
            """
            cursor.execute(create_table_sql)
            connection.commit()
            print("表 product_monitor 创建成功！")
    finally:
        connection.close()

def import_multiple_csv_to_product_monitor(csv_dir: str):
    """导入目录下所有 upload*.csv 到 product_monitor"""
    csv_dir_path = Path(csv_dir)
    files = list(csv_dir_path.glob("upload*.csv"))
    if not files:
        print("没有找到以 upload 开头的 CSV 文件！")
        return

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

            total_inserted = 0
            for file in files:
                print(f"开始导入 {file}")
                df = pd.read_csv(file)
                # 清理列名空格
                df.columns = df.columns.str.strip()
                # 转换日期
                for col in ['发现日期', '上架日期']:
                    if col in df.columns:
                        df[col] = df[col].apply(parse_date)

                for _, row in df.iterrows():
                    cursor.execute(insert_sql, (
                        safe_value(row.get('发现日期')),
                        safe_value(row.get('来源平台')),
                        safe_value(row.get('商品ID')),
                        safe_value(row.get('图片')),
                        str(safe_value(row.get('产品名称')))[:1000],
                        safe_value(row.get('上架日期')),
                        safe_int(row, '总销量'),
                        safe_int(row, '月销量'),
                        safe_value(row.get('托管模式')),
                        safe_value(row.get('在售站点')),
                        safe_value(row.get('产品链接')),
                        safe_value(row.get('类目'))
                    ))

                total_inserted += len(df)
                connection.commit()
                print(f"{file.name} 导入完成，共 {len(df)} 条数据")

            print(f"总共插入 {total_inserted} 条数据到 product_monitor 表！")
    finally:
        connection.close()


# ---------------- 示例用法 ----------------
if __name__ == "__main__":
    create_product_monitor_table()
    import_multiple_csv_to_product_monitor(r"D:\sd14\New_Ymx\data\data")
