import pandas as pd
import json

# 读取 Excel 文件
df = pd.read_excel('goods_attribute.xlsx')

# 查看列名
print("列名:", df.columns.tolist())
print(f"总行数: {len(df)}")

# 构建配置字典
category_config = {}

# 按类目ID分组
for cid, group in df.groupby('类目ID'):
    cid_str = str(cid)

    # 获取类目名称（取第一条）
    category_name = group.iloc[0]['类目名称']

    # 构建该类的属性列表
    attributes_dict = {}

    for _, row in group.iterrows():
        attr_name = row['属性名称']
        attr_value = row['属性值']

        if attr_name not in attributes_dict:
            attributes_dict[attr_name] = {
                "name": attr_name,
                "values": []
            }

        # 添加属性值（去重）
        value_exists = False
        for v in attributes_dict[attr_name]["values"]:
            if v["name"] == attr_value:
                value_exists = True
                break

        if not value_exists:
            attributes_dict[attr_name]["values"].append({
                "name": attr_value,
                "valueUnit": ""
            })

    # 转换为列表格式
    attributes_list = list(attributes_dict.values())

    # 构建配置
    category_config[cid_str] = {
        "category_name": category_name,
        "attributes": attributes_list,
    }

# 打印结果预览
print("\n" + "=" * 50)
print("生成的配置预览:")
print("=" * 50)

for cid, config in list(category_config.items())[:3]:  # 只显示前3个
    print(f"\n类目ID: {cid}")
    print(f"类目名称: {config['category_name']}")
    print(f"属性数量: {len(config['attributes'])}")
    for attr in config['attributes']:
        print(f"  - {attr['name']}: {[v['name'] for v in attr['values']]}")

# 保存为 Python 文件
output_py = """# config/category_attributes.py
# 自动生成的类目属性配置

CATEGORY_ATTRIBUTES = """

output_py += json.dumps(category_config, ensure_ascii=False, indent=4)

# 替换 JSON 格式为 Python 字典格式
output_py = output_py.replace('"', "'")  # 双引号替换为单引号
output_py = output_py.replace("'name':", '"name":')  # 但 name 键保持双引号
output_py = output_py.replace("'values':", '"values":')
output_py = output_py.replace("'category_name':", '"category_name":')
output_py = output_py.replace("'attributes':", '"attributes":')
output_py = output_py.replace("'package':", '"package":')
output_py = output_py.replace("'outerPackageShape':", '"outerPackageShape":')
output_py = output_py.replace("'outerPackageType':", '"outerPackageType":')
output_py = output_py.replace("'length':", '"length":')
output_py = output_py.replace("'width':", '"width":')
output_py = output_py.replace("'height':", '"height":')
output_py = output_py.replace("'weight':", '"weight":')

# 更简单的方式：直接保存为 JSON，然后在 Python 中导入
with open('category_attributes.json', 'w', encoding='utf-8') as f:
    json.dump(category_config, f, ensure_ascii=False, indent=4)

print(f"\n✅ 已生成配置文件: category_attributes.json")
print(f"✅ 共处理 {len(category_config)} 个类目")

# 生成统计信息
print("\n📊 统计信息:")
print(f"  类目总数: {len(category_config)}")
total_attrs = sum(len(c['attributes']) for c in category_config.values())
print(f"  属性总数: {total_attrs}")

# 列出所有类目
print("\n📋 所有类目:")
for cid, config in category_config.items():
    print(f"  {cid}: {config['category_name']} ({len(config['attributes'])}个属性)")