import json
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "config.json"

def load_config():
    with open(CONFIG_DIR, "r",encoding="utf-8") as f:
        return json.load(f)

def get_shop_config(site_name):
    config = load_config()
    accout=config.get(site_name)
    return accout

def get_dingtalk_config():
    """
    获取钉钉全局配置
    """
    config = load_config()
    dingtalk = config.get("dingding")

    if not dingtalk:
        raise ValueError("config.json 中未配置 dingtalk")

    return dingtalk



