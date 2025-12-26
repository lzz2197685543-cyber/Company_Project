import pandas as pd
from datetime import datetime

def simple_match(shop_name):
    current_date = datetime.now().strftime("%Y%m%d")

    # 读取文件
    sku_df = pd.read_csv(f'./data/data/{shop_name}_goods_{current_date}.csv')  # 货号ID,sku
    main_df = pd.read_csv(f'./data/data/{shop_name}_stock_{current_date}.csv')  # 平台,店铺,货号ID,商品名称,...

    sku_df['货号ID'] = sku_df['货号ID'].astype(str)
    main_df['货号ID'] = main_df['货号ID'].astype(str)

    # 使用merge合并数据
    result_df = pd.merge(
        main_df,
        sku_df,
        on='货号ID',
        how='left'
    )


    records=[]
    # 修复：解包 iterrows() 返回的元组
    for index, row in result_df.iterrows():
        # 检查指定字段是否都为0
        if (row['今日销量'] == 0 and
                row['近7天销量'] == 0 and
                row['近30天销量'] == 0 and
                row['平台库存'] == 0 and
                row['在途库存'] == 0):
            continue  # 跳过这条记录

        record = {
            'ID':row['货号ID'],
            "商品名称": row['商品名称'],
            "抓取数据日期": row['抓取数据日期'],
            "今日销量": row['今日销量'],
            "近7天销量": row['近7天销量'],
            "近30天销量": row['近30天销量'],
            "平台库存": row['平台库存'],
            "平台": row['平台'],
            "在途库存": row['在途库存'],
            "sku": str(row['sku']) if not pd.isna(row['sku']) else "",
            "店铺": row['店铺'],
        }
        records.append(record)

    return records

for i in simple_match('SMT202'):
    print(i)