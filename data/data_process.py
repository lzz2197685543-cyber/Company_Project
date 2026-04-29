import pandas as pd
import json

# 方法1：逐行读取（每行一个JSON对象）
data_list = []

with open('publish_fails.jsonl', 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:  # 跳过空行
            try:
                data_list.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"解析错误: {e}")
                continue

# 转换为DataFrame
df = pd.DataFrame(data_list)

# 展开嵌套的response字段
if 'response' in df.columns:
    response_df = df['response'].apply(pd.Series).add_prefix('response_')
    df = pd.concat([df.drop(columns=['response']), response_df], axis=1)

df.drop_duplicates(subset=['cid'],keep='first', inplace=True)

# 保存为CSV
df.to_csv('output.csv', index=False, encoding='utf-8-sig')
print(f"✅ 转换完成！共 {len(df)} 行数据")
print(df.head())


# import pandas as pd
#
# df=pd.read_csv('category_info.csv')
# df.drop_duplicates(subset=['类目ID'],keep='first', inplace=True)
# df.to_csv('output1.csv', index=False, encoding='utf-8-sig')