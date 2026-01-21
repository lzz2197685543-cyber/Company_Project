import base64
import json
import asyncio
import requests
from pathlib import Path
from typing import Optional, Dict, Any, List
import aiohttp
import time
from auth.captcha.base import BaseCaptchaProcessor, CONFIG_DIR


class YunmaCaptchaAPI:
    """云码API封装类"""

    # 验证码类型映射
    CAPTCHA_TYPE_MAP = {
        "tencent_slider": "22222",  # 腾讯滑块
        "tencent_text_click": "30100",  # 腾讯文字点选
        "tencent_image_click": "30340"  # 腾讯图片点选
    }

    def __init__(self, api_url: str = "http://api.jfbym.com/api/YmServer/customApi"):
        """
        初始化云码API

        Args:
            token: 云码平台token
            api_url: API地址
        """
        self.token = "WVX-0-2uHkuJqkSYHkETIJZuUT_VduvtC_OUWu73kQ0"
        self.api_url = api_url
        self.headers = {
            "Content-Type": "application/json"
        }

    def image_to_base64(self, image_path: str) -> str:
        """图片转base64"""
        try:
            with open(image_path, 'rb') as f:
                return base64.b64encode(f.read()).decode()
        except Exception as e:
            raise Exception(f"图片转换失败: {e}")

    def identify_slider(self, image_path: str) -> Dict[str, Any]:
        """识别滑块验证码"""
        try:
            image_base64 = self.image_to_base64(image_path)

            data = {
                "token": self.token,
                "type": self.CAPTCHA_TYPE_MAP["tencent_slider"],
                "image": image_base64,
            }

            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=data,
                timeout=30
            )

            result = response.json()
            return self._parse_result(result, "slider")

        except Exception as e:
            return {"code": -1, "message": f"滑块识别失败: {e}"}

    def identify_text_click(self, bg_image_path: str, words: List[str]) -> Dict[str, Any]:
        """识别文字点选验证码"""
        try:
            image_base64 = self.image_to_base64(bg_image_path)

            data = {
                "token": self.token,
                "type": self.CAPTCHA_TYPE_MAP["tencent_text_click"],
                "image": image_base64,
                "extra":  ','.join(words),  # 需要点击的文字列表

            }

            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=data,
                timeout=30
            )

            result = response.json()
            return self._parse_result(result, "text_click")

        except Exception as e:
            return {"code": -1, "message": f"文字点选识别失败: {e}"}

    def identify_image_click(self, screen_path) -> Dict[str, Any]:
        """识别图片点选验证码"""
        try:

            # 读取答案图片
            with open(screen_path, 'rb') as f:
                img = base64.b64encode(f.read()).decode()

            data = {
                "token": self.token,
                "type": self.CAPTCHA_TYPE_MAP["tencent_image_click"],
                "image": img,
            }

            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=data,
                timeout=30
            )

            result = response.json()
            return self._parse_result(result, "image_click")

        except Exception as e:
            return {"code": -1, "message": f"图片点选识别失败: {e}"}

    def _parse_points(self, raw: str):
        """
        把 '154,268|192,188|74,229'
        转成 [{'x':154,'y':268}, ...]
        """
        points = []
        for item in raw.split("|"):
            try:
                x, y = item.split(",")
                points.append({"x": int(x), "y": int(y)})
            except ValueError:
                continue
        return points

    def _parse_result(self, result: Dict, captcha_type: str) -> Dict[str, Any]:
        """解析API返回结果（支持点选 / 文字点选 / 滑块）"""

        if not isinstance(result, dict):
            return {"code": -1, "message": "API返回格式错误"}

        # ---------- 外层 ----------
        outer_code = result.get("code")
        outer_msg = result.get("msg", "")

        if outer_code != 10000:
            return {
                "code": outer_code or -1,
                "type": captcha_type,
                "message": f"接口调用失败: {outer_msg}",
                "data": result
            }

        # ---------- 内层 ----------
        data = result.get("data", {})
        inner_code = data.get("code")
        raw_data = data.get("data")

        if inner_code != 0:
            return {
                "code": inner_code,
                "type": captcha_type,
                "message": "识别失败",
                "data": data
            }

        # ---------- 按验证码类型解析 ----------
        parsed = {}

        if captcha_type in ("image_click", "text_click"):
            # 点选 / 文字点选
            parsed["points"] = self._parse_points(raw_data)
            parsed["raw"] = raw_data

        elif captcha_type == "slider":
            # 滑块
            parsed["distance"] = int(raw_data)
            parsed["raw"] = raw_data

        else:
            parsed["raw"] = raw_data

        return {
            "code": 0,
            "type": captcha_type,
            "message": "识别成功",
            "data": parsed
        }
