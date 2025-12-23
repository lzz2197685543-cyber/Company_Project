import json
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "config.json"

def load_config():
    with open(CONFIG_DIR, "r",encoding="utf-8") as f:
        return json.load(f)

def get_shop_config(shop_name):
    config = load_config()
    shops=config.get('shops',{})
    if shop_name not in shops:
        raise ValueError(f"未找到店铺配置: {shop_name}")
    return shops[shop_name]

