import pandas as pd

df = pd.read_csv('./category_info.csv')
df1 = pd.read_csv('./goods_attributes.csv')

# 将叶子类目名称映射到df1中
df1 = df1.merge(df[['类目ID', '叶子类目名称']], on='类目ID', how='left')

# 保存到新的CSV文件
df1.to_csv('./goods_attributes_with_category.csv', index=False, encoding='utf-8-sig')

print("已保存为: goods_attributes_with_category.csv")
print(f"保存成功，共 {len(df1)} 行数据")