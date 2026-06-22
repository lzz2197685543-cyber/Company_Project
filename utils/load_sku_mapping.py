import pickle
from pathlib import Path
from datetime import datetime
import pandas as pd

FILE_DIR1 = Path(__file__).resolve().parent.parent.parent / "data"

# 调试模式开关
DEBUG_MODE = True  # 改为True方便调试


def get_prev_month_from_now() -> tuple:
    """
    返回当前时间的前一个月，返回两个值：(YYYY-MM, MM)
    """
    now = datetime.now()
    year = now.year
    month = now.month

    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1

    return f"{year}{month:02d}", f"{month:02d}"


year, month = get_prev_month_from_now()


def get_cache_file_path():
    """获取缓存文件路径"""
    cache_dir = Path(__file__).resolve().parent.parent.parent / "data" / f"{month}月份" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"cost_mapping_{year}.pkl"

def load_cost_mapping(use_cache=True):
    """
    加载成本映射表，支持缓存

    参数:
        use_cache: 是否使用缓存，默认True

    返回两个字典：
        - single_sku_cost: 普通商品编码 -> 成本价
        - combined_sku_cost: 组合商品编码 -> 组合成本价
    """
    cache_file = get_cache_file_path()

    # 如果使用缓存且缓存文件存在，直接加载
    if use_cache and cache_file.exists():
        try:
            with open(cache_file, 'rb') as f:
                single_sku_cost, combined_sku_cost = pickle.load(f)
            print(f"  从缓存加载成本映射: 普通商品 {len(single_sku_cost)} 条, 组合商品 {len(combined_sku_cost)} 条")
            return single_sku_cost, combined_sku_cost
        except Exception as e:
            print(f"  缓存加载失败: {e}，重新生成...")

    # 重新生成映射
    single_sku_file = FILE_DIR1 / f"{month}月份" / f'商品及库存管理_{year}.xlsx'
    combined_sku_file = list((FILE_DIR1 / f"{month}月份").glob("组合装商品*.xlsx"))[0] if list(
        (FILE_DIR1 / f"{month}月份").glob("组合装商品*.xlsx")) else None

    single_sku_cost = {}
    combined_sku_cost = {}

    # 读取普通商品成本表
    try:
        single_df = pd.read_excel(single_sku_file)
        if '商品编码' in single_df.columns and '成本价' in single_df.columns:
            # 去除空值
            single_df = single_df[single_df['商品编码'].notna() & single_df['成本价'].notna()]
            single_sku_cost = dict(zip(single_df['商品编码'].astype(str), single_df['成本价']))
            print(f"  普通商品成本映射: {len(single_sku_cost)} 条")
        else:
            print(f"  警告：普通商品成本表列名不匹配，可用列名: {single_df.columns.tolist()}")
    except FileNotFoundError:
        print(f"  警告：找不到普通商品成本表文件 {single_sku_file}")
    except Exception as e:
        print(f"  读取普通商品成本表错误：{e}")

    # 读取组合商品成本表
    try:
        combined_df = pd.read_excel(combined_sku_file)
        if '组合商品编码' in combined_df.columns and '组合成本价' in combined_df.columns:
            # 去除空值
            combined_df = combined_df[combined_df['组合商品编码'].notna() & combined_df['组合成本价'].notna()]
            combined_sku_cost = dict(zip(combined_df['组合商品编码'].astype(str), combined_df['组合成本价']))
            print(f"  组合商品成本映射: {len(combined_sku_cost)} 条")
        else:
            print(f"  警告：组合商品成本表列名不匹配，可用列名: {combined_df.columns.tolist()}")
    except FileNotFoundError:
        print(f"  警告：找不到组合商品成本表文件 {combined_sku_file}")
    except Exception as e:
        print(f"  读取组合商品成本表错误：{e}")

    # 保存到缓存
    if use_cache and single_sku_cost and combined_sku_cost:
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump((single_sku_cost, combined_sku_cost), f)
            print(f"  成本映射已保存到缓存: {cache_file}")
        except Exception as e:
            print(f"  缓存保存失败: {e}")

    return single_sku_cost, combined_sku_cost

def normalize_sku(sku: str) -> str:
    """
    标准化SKU编码，处理Excel转义字符

    将 + 替换为 _x002B_，反之亦然
    """
    if not sku:
        return sku
    # 将 + 替换为 _x002B_
    normalized = sku.replace('+', '_x002B_')
    # 将 _x002B_ 替换为 +（反向）
    # normalized = sku.replace('_x002B_', '+')
    return normalized

def get_cost_price(sku, single_sku_cost, combined_sku_cost):
    """
    获取单个SKU的成本价
    优先匹配普通商品，匹配不到再匹配组合商品
    支持忽略大小写匹配

    参数:
        sku: SKU货号
        single_sku_cost: 普通商品成本映射
        combined_sku_cost: 组合商品成本映射

    返回:
        成本价，如果都没匹配到返回None
    """
    if sku is None or pd.isna(sku):
        return None

    sku_str = str(sku).strip()
    if not sku_str:
        return None

    # 生成可能的SKU变体列表
    sku_variants = [
        sku_str,  # 原始
        sku_str.lower(),  # 小写
        sku_str.replace('+', '_x002B_'),  # + -> _x002B_
        sku_str.replace('+', '_x002B_').lower(),  # + -> _x002B_ 且小写
        sku_str.replace('_x002B_', '+'),  # _x002B_ -> +
        sku_str.replace('_x002B_', '+').lower(),  # _x002B_ -> + 且小写
    ]
    # 去重
    sku_variants = list(dict.fromkeys(sku_variants))

    # if DEBUG_MODE:
    #     print(f"  [DEBUG] 查找SKU: '{sku_str}'")
    #     print(f"  [DEBUG] 变体列表: {sku_variants}")

    # 1. 先尝试普通商品匹配
    for variant in sku_variants:
        cost = single_sku_cost.get(variant)
        if cost is not None:
            # if DEBUG_MODE:
            #     print(f"  [DEBUG] 普通商品匹配成功: {variant} -> {cost}")
            return float(cost)

    # 2. 再尝试组合商品匹配
    for variant in sku_variants:
        cost = combined_sku_cost.get(variant)
        if cost is not None:
            if DEBUG_MODE:
                print(f"  [DEBUG] 组合商品匹配成功: {variant} -> {cost}")
            return float(cost)

    # 3. 如果还是没匹配到，尝试普通商品的忽略大小写匹配
    for variant in sku_variants:
        variant_lower = variant.lower()
        for key, value in single_sku_cost.items():
            if key.lower() == variant_lower:
                if DEBUG_MODE:
                    print(f"  [DEBUG] 普通商品忽略大小写匹配成功: {key} -> {value}")
                return float(value)

    # 4. 尝试组合商品的忽略大小写匹配
    for variant in sku_variants:
        variant_lower = variant.lower()
        for key, value in combined_sku_cost.items():
            if key.lower() == variant_lower:
                if DEBUG_MODE:
                    print(f"  [DEBUG] 组合商品忽略大小写匹配成功: {key} -> {value}")
                return float(value)

    # 如果开启调试模式，打印未匹配的信息
    if DEBUG_MODE:
        print(f"  [DEBUG] 未匹配SKU: '{sku_str}'")
        print(f"  [DEBUG] 组合商品示例: {list(combined_sku_cost.keys())[:3]}")

    return None


def refresh_cost_cache():
    """
    强制刷新成本映射缓存
    """
    print("正在刷新成本映射缓存...")
    cache_file = get_cache_file_path()
    if cache_file.exists():
        cache_file.unlink()
        print(f"  已删除旧缓存: {cache_file}")

    # 重新加载并保存缓存
    return load_cost_mapping(use_cache=True)


if __name__ == '__main__':
    # 开启调试模式
    DEBUG_MODE = True

    single_sku_cost, combined_sku_cost = load_cost_mapping(use_cache=True)

    # 测试匹配
    test_sku = "SYWJ-pk+bl"
    result = get_cost_price(test_sku, single_sku_cost, combined_sku_cost)
    print(f"\n=== 测试结果 ===")
    print(f"SKU: {test_sku}")
    print(f"匹配结果: {result}")

    # 也测试一下转义后的版本
    test_sku_escaped = "SYWJ-pk_x002B_bl"
    result2 = get_cost_price(test_sku_escaped, single_sku_cost, combined_sku_cost)
    print(f"\nSKU: {test_sku_escaped}")
    print(f"匹配结果: {result2}")