# 验证码抽象类

import re
import requests
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "img"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)


class BaseCaptchaProcessor(ABC):
    """验证码处理器基类"""

    CAPTCHA_SELECTORS = {
        "click": ".tencent-captcha-dy__click-type-wrap",
        "slider": ".tencent-captcha-dy__slider-type-wrap",
    }

    def __init__(self, logger):
        self.logger = logger

    @staticmethod
    def download_image(url: str, filepath: Path) -> bool:
        """下载图片到本地"""
        try:
            response = requests.get(url)
            with open(filepath, 'wb') as f:
                f.write(response.content)
            return True
        except Exception as e:
            print(f"下载图片失败: {e}")
            return False

    @staticmethod
    def extract_bg_image_url(css_style: str) -> Optional[str]:
        """从CSS样式中提取背景图片URL"""
        if not css_style:
            return None

        pattern = r'background-image:\s*url\("([^"]+)"\)'
        match = re.search(pattern, css_style)
        return match.group(1) if match else None

    @abstractmethod
    async def wait_and_identify_captcha(self, page, timeout: int = 10000):
        """等待并识别验证码类型"""
        pass

    @abstractmethod
    async def analyze_click_captcha(self, page):
        """分析点击型验证码"""
        pass

    @abstractmethod
    async def handle_captcha(self, page, captcha_type: str) -> Optional[Dict[str, Any]]:
        """处理验证码"""
        pass
