import json
from pathlib import Path
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "config.json"

def load_config():
    with open(CONFIG_PATH, "r",encoding="utf-8") as f:
        return json.load(f)


def get_shop_config(shop_name:str):
    config = load_config()
    shops = config.get('shops',{})
    if shop_name not in shops:
        raise ValueError(f"未找到店铺配置: {shop_name}")
    return shops[shop_name]


def get_dingtalk_config():
    """
    获取钉钉全局配置
    """
    config = load_config()
    dingtalk = config.get("dingding")

    if not dingtalk:
        raise ValueError("config.json 中未配置 dingtalk")

    return dingtalk

