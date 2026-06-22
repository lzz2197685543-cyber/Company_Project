from pathlib import Path
import pandas as pd

# 定义月份列表
month_list = ['01', '02', '03', '04', '05']

# 存储所有数据的列表
all_data = []

for month in month_list:
    # 构建文件夹路径
    folder_path = Path(__file__).resolve().parent / "data" / "financial" / f'{month}月份'

    # 检查文件夹是否存在
    if not folder_path.exists():
        print(f"警告: 文件夹 {folder_path} 不存在，跳过")
        continue

    # 遍历文件夹中所有xlsx文件（只包含美国的）
    for file_path in folder_path.glob('*美国.xlsx'):
        try:
            # 读取交易结算子表（假设子表名称就是"交易结算"）
            df = pd.read_excel(file_path, sheet_name='交易结算')

            # 从文件名提取店铺名称和月份
            # 文件名格式: 2101-Temu全托管KA_01_美国.xlsx
            file_name = file_path.stem  # 去掉扩展名
            parts = file_name.split('_')

            # 店铺名称: 第一个部分到倒数第三个部分（去掉月份和"美国"）
            # 例如: 2101-Temu全托管KA_01_美国 -> 店铺名称 = 2101-Temu全托管KA
            store_name = '_'.join(parts[:-2])  # 去掉最后两个部分（月份和"美国"）
            file_month = parts[-2]  # 获取月份

            # 添加月份列和店铺列
            df['月份'] = file_month
            df['店铺名称'] = store_name

            # 添加到总数据列表
            all_data.append(df)

            print(f"成功读取: {file_path.name} (店铺: {store_name}, 月份: {file_month})")

        except Exception as e:
            print(f"读取文件 {file_path.name} 时出错: {e}")

# 合并所有数据
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n总共合并了 {len(all_data)} 个文件的数据，共 {len(combined_df)} 行")

    # 可选：保存合并后的数据
    combined_df.to_excel('合并数据.xlsx', index=False)
else:
    print("没有读取到任何数据")