import pandas as pd
from utils.dingding_doc import DingTalkTokenManager,DingTalkSheetUploader

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import re

# 数据目录
data_dir = Path("/mnt/data/temu_excels")  # 改成你存放 Excel 的目录

# 全局字典，聚合所有 SKU
records_all = {}

# 遍历所有 Excel 文件
for file_path in data_dir.glob("*.xlsx"):
    # 从文件名提取信息：店铺、月份、区域
    # 例：101-Temu全托管_12_卖家中心.xlsx
    m = re.match(r".*_(\d+)_([^\._]+)\.xlsx", file_path.name)
    if not m:
        print("文件名无法解析:", file_path.name)
        continue
    month_str, region_name = m.groups()
    month = int(month_str)

    # 读取 Excel
    xls = pd.ExcelFile(file_path)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name)
        df = df.fillna(0)

        # ---- 处理交易结算 ----
        if sheet_name == "交易结算":
            for _, row in df.iterrows():
                sku = row["SKU货号"]
                amount = row["金额"]
                quantity = row["数量"]

                if sku not in records_all:
                    records_all[sku] = {
                        "平台": "temu",
                        "店铺": "YourShopName",
                        "sku货号": sku,
                        "交易收入-销售数量": 0,
                        "交易收入-收入金额": 0,
                        "退款数量": 0,
                        "退款金额-赔付金额": 0,
                        "售后补贴数量": 0,
                        "售后补贴-补贴金额": 0,
                        "售后补贴-补贴金额调整": 0,
                        "售后问题-数量": 0,
                        "售后问题-赔付金额": 0,
                        "售后补寄-数量": 0,
                        "售后补寄-赔付金额": 0,
                        "核价": [],
                        "月份": f"{datetime.now().year}年{month}月",
                    }

                records_all[sku]["交易收入-销售数量"] += quantity
                records_all[sku]["交易收入-收入金额"] += amount
                if quantity > 0:
                    records_all[sku]["核价"].append(amount / quantity)

        # ---- 处理消费者退款 ----
        elif sheet_name == "结算-消费者退款金额":
            for _, row in df.iterrows():
                sku = row["SKU货号"]
                refund_amount = row["消费者退款金额"]

                if sku not in records_all:
                    records_all[sku] = {
                        "平台": "temu",
                        "店铺": "YourShopName",
                        "sku货号": sku,
                        "交易收入-销售数量": 0,
                        "交易收入-收入金额": 0,
                        "退款数量": 0,
                        "退款金额-赔付金额": 0,
                        "售后补贴数量": 0,
                        "售后补贴-补贴金额": 0,
                        "售后补贴-补贴金额调整": 0,
                        "售后问题-数量": 0,
                        "售后问题-赔付金额": 0,
                        "售后补寄-数量": 0,
                        "售后补寄-赔付金额": 0,
                        "核价": [],
                        "月份": f"{datetime.now().year}年{month}月",
                    }

                records_all[sku]["退款数量"] += 1
                records_all[sku]["退款金额-赔付金额"] += refund_amount

        # 其他 Sheet 可按同样逻辑继续补充
        # 比如：
        # "结算-非商责平台售后补贴金额"
        # "消费者及履约保障-售后问题"
        # "消费者及履约保障-售后补寄"
        # "非商责平台售后补贴调整"

# ---- 汇总最终 DataFrame ----
final_list = []
for sku, data in records_all.items():
    data["最低核价"] = min(data["核价"]) if data["核价"] else 0
    data["平均核价"] = np.mean(data["核价"]) if data["核价"] else 0
    data.pop("核价")  # 删除中间字段
    final_list.append(data)

df_final = pd.DataFrame(final_list)

# 查看结果
print(df_final.head())
