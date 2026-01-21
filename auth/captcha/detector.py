# 验证码类型判断
import json
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any
from auth.captcha.base import BaseCaptchaProcessor, CONFIG_DIR
from auth.captcha.jfbym import YunmaCaptchaAPI
import random


class YunmaCaptchaProcessor(BaseCaptchaProcessor):
    """云码平台验证码处理器"""

    def __init__(self, logger):
        super().__init__(logger)
        self.jfbym = YunmaCaptchaAPI()

    async def wait_and_identify_captcha(self, page, timeout: int = 10000) -> Optional[str]:
        """等待验证码出现并识别类型"""
        try:
            # 等待任意一种验证码出现
            selector = ", ".join(self.CAPTCHA_SELECTORS.values())
            await page.wait_for_selector(selector, timeout=timeout)

            # 检查具体类型
            for captcha_type, selector in self.CAPTCHA_SELECTORS.items():
                if await page.locator(selector).count() > 0:
                    return captcha_type

            return None
        except Exception as e:
            self.logger.error(f"等待验证码失败: {e}")
            return None

    async def analyze_click_captcha(self, page) -> Optional[Dict[str, Any]]:
        """分析点击型验证码（文字或图片）"""
        try:
            # 获取验证码头部信息
            header_wrap = page.locator(".tencent-captcha-dy__header-wrap")

            # 检查是否有答案图片
            answer_img = header_wrap.locator(".tencent-captcha-dy__header-answer img")
            answer_img_count = await answer_img.count()

            # 获取提示文字
            text_element = header_wrap.locator(".tencent-captcha-dy__header-text")
            text_content = await text_element.text_content()

            captcha_info = {
                "type": None,
                "prompt_text": text_content,
                "words": None,
                "bg_image_url": None,
                "answer_img_url": None,
                "local_bg_path": None,
                "local_answer_path": None,
                'screenshot_path': None,
            }

            # 判断具体类型
            if answer_img_count > 0:
                # 图片点击验证码
                captcha_info["type"] = "image_click"
                answer_img_url = await answer_img.get_attribute('src')
                captcha_info["answer_img_url"] = answer_img_url
                self.logger.info(f"检测到图片点击验证码，答案图片URL: {answer_img_url}")

            elif text_content and '请依次点击' in text_content:
                # 文字点击验证码
                captcha_info["type"] = "text_click"
                words = text_content.replace("请依次点击：", "").strip().split()
                captcha_info["words"] = words
                self.logger.info(f"检测到文字点击验证码，需要点击的文字: {words}")

            else:
                # 未知类型
                self.logger.warning("未知点击验证码类型")
                return None

            # 获取背景图片
            bg_image_url = await self._get_background_image_url(page)
            if bg_image_url:
                captcha_info["bg_image_url"] = bg_image_url

                # 下载背景图片
                bg_filename = CONFIG_DIR / f'captcha_{captcha_info["type"]}_bg.jpg'
                if self.download_image(bg_image_url, bg_filename):
                    captcha_info["local_bg_path"] = str(bg_filename)
                    self.logger.info(f"背景图片已保存: {bg_filename}")

            # 下载答案图片（如果是图片点击验证码）
            if captcha_info["type"] == "image_click" and captcha_info["answer_img_url"]:
                captcha = page.locator(
                    ".tencent-captcha-dy__warp.tencent-captcha-dy__click-type-wrap"
                )

                await captcha.wait_for(timeout=30_000)

                # 对该元素截图
                await captcha.screenshot(path=f"{CONFIG_DIR}/tencent_captcha.png")
                captcha_info["screenshot_path"] = f"{CONFIG_DIR}/tencent_captcha.png"
                self.logger.info('已截图')

                answer_filename = CONFIG_DIR / 'captcha_answer.jpg'

                if self.download_image(captcha_info["answer_img_url"], answer_filename):
                    captcha_info["local_answer_path"] = str(answer_filename)
                    self.logger.info(f"答案图片已保存: {answer_filename}")

            if captcha_info["type"] == "text_click":
                captcha = page.locator(
                    ".tencent-captcha-dy__verify-bg-img"
                )

                await captcha.wait_for(timeout=30_000)

                # 对该元素截图
                await captcha.screenshot(path=f"{CONFIG_DIR}/tencent_text_captcha.png")
                captcha_info["screenshot_path"] = f"{CONFIG_DIR}/tencent_text_captcha.png"
                self.logger.info('已截图')

            return captcha_info

        except Exception as e:
            self.logger.error(f"分析点击验证码失败: {e}")
            return None

    async def _get_background_image_url(self, page) -> Optional[str]:
        """获取验证码背景图片URL"""
        try:
            img_element = page.locator(".tencent-captcha-dy__verify-bg-img")
            css_style = await img_element.get_attribute("style")
            return self.extract_bg_image_url(css_style)
        except Exception as e:
            self.logger.error(f"获取背景图片URL失败: {e}")
            return None

    async def handle_captcha(self, page, captcha_type: str) -> Optional[Dict[str, Any]]:
        """根据验证码类型处理"""
        if captcha_type == "click":
            # 处理点击型验证码
            return await self.analyze_click_captcha(page)

        elif captcha_type == "slider":
            # 处理滑块验证码
            self.logger.info("检测到滑块验证码")
            slider_text = await page.locator(".tencent-captcha-dy__header-text").text_content()
            self.logger.info(f"滑块提示: {slider_text}")


            # 获取背景图片
            bg_image_url = await self._get_background_image_url(page)
            if bg_image_url:
                captcha = page.locator(
                    ".tencent-captcha-dy__image-area"
                )

                await captcha.wait_for(timeout=30_000)

                # 对该元素截图
                await captcha.screenshot(path=f"{CONFIG_DIR}/tencent_captcha_slider.png")

                bg_filename = CONFIG_DIR / 'tencent_captcha_slider.png'

                return {
                    "type": "slider",
                    "prompt_text": slider_text,
                    "local_bg_path": str(bg_filename)
                }

            return {"type": "slider", "prompt_text": slider_text}

        else:
            self.logger.warning(f"未知的验证码类型: {captcha_type}")
            return None

    async def send_to_yunma_api(self, page,captcha_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """发送到云码平台API"""
        try:
            # 根据验证码类型准备不同的数据
            if captcha_info["type"] == "text_click":
                payload = {
                    "type": "tencent_text_click",
                    "image": captcha_info.get("local_bg_path"),
                    "words": captcha_info.get("words", []),
                    "prompt_text": captcha_info.get("prompt_text"),
                    "screenshot_path": captcha_info.get("screenshot_path")
                }
                # 调用云码，返回结果
                result=self.jfbym.identify_text_click(payload['screenshot_path'], payload['words'])
                # 1️⃣ 点选验证码
                await self.click_captcha_points(
                    page,
                    result["data"]["points"],
                    img_path='.tencent-captcha-dy__verify-bg-img'
                )

                # 2️⃣ 稍等（非常重要）
                await asyncio.sleep(random.uniform(0.4, 0.8))

                # 3️⃣ 点击【确定】
                await self.click_confirm_button(page)
                print(result)

            elif captcha_info["type"] == "image_click":
                payload = {
                    "type": "tencent_image_click",
                    "bg_image": captcha_info.get("local_bg_path"),
                    "prompt_image": captcha_info.get("local_answer_path"),
                    "prompt_text": captcha_info.get("prompt_text"),
                    "screenshot_path": captcha_info.get("screenshot_path")
                }
                # 调用云码，返回结果
                result = self.jfbym.identify_image_click(payload['screenshot_path'])
                # 1️⃣ 点选验证码
                await self.click_captcha_points(
                    page,
                    result["data"]["points"],
                    img_path=".tencent-captcha-dy__warp.tencent-captcha-dy__click-type-wrap"
                )

                # 2️⃣ 稍等（非常重要）
                await asyncio.sleep(random.uniform(0.4, 0.8))

                # 3️⃣ 点击【确定】
                await self.click_confirm_button(page)

            elif captcha_info["type"] == "slider":
                payload = {
                    "type": "tencent_slider",
                    "image": captcha_info.get("local_bg_path"),
                    "prompt_text": captcha_info.get("prompt_text")
                }
                # 调用云码，返回结果
                result = self.jfbym.identify_slider(payload['image'])
                await self.drag_slider_human_like(page,result['data']['distance'])
            else:
                return None

        except Exception as e:
            self.logger.error(f"发送到云码API失败: {e}")
            return None

    # 文字点击处理
    async def click_captcha_points(self, page, points,img_path):
        """
        使用【验证码整体容器】作为坐标基准，100% 对齐云码
        """
        container = page.locator(
            img_path
        )
        await container.wait_for(timeout=10_000)

        box = await container.bounding_box()
        if not box:
            raise RuntimeError("无法获取验证码容器 bounding box")

        # DPR 修正
        dpr = await page.evaluate("window.devicePixelRatio") or 1

        left = box["x"]
        top = box["y"]

        self.logger.info(f"验证码容器: {box}, DPR={dpr}")

        for idx, point in enumerate(points, 1):
            # ⚠️ 云码是物理像素 → 除以 DPR
            click_x = left + point["x"] / dpr
            click_y = top + point["y"] / dpr

            # 微随机
            click_x += random.uniform(-2, 2)
            click_y += random.uniform(-2, 2)

            self.logger.info(
                f"点击第 {idx} 个点: ({click_x:.1f}, {click_y:.1f})"
            )

            await page.mouse.move(click_x, click_y, steps=10)
            await asyncio.sleep(random.uniform(0.25, 0.45))
            await page.mouse.click(click_x, click_y)
            await asyncio.sleep(random.uniform(0.4, 0.7))

    # 点击“确认”
    async def click_confirm_button(self, page):
        """
        点击腾讯验证码的【确定】按钮（拟人）
        """
        btn = page.locator(".tencent-captcha-dy__verify-confirm-btn")
        await btn.wait_for(timeout=5_000)

        box = await btn.bounding_box()
        if not box:
            raise RuntimeError("无法获取确定按钮 bounding box")

        # 按钮中心 + 微随机
        x = box["x"] + box["width"] / 2 + random.uniform(-3, 3)
        y = box["y"] + box["height"] / 2 + random.uniform(-2, 2)

        await asyncio.sleep(random.uniform(0.3, 0.6))
        await page.mouse.move(x, y, steps=12)
        await asyncio.sleep(random.uniform(0.1, 0.25))
        await page.mouse.click(x, y)

    # 人类轨迹生成函数
    def _generate_slider_track(self, distance):
        """
        生成符合人类行为的滑块轨迹
        return: [(dx, dy, delay), ...]
        """
        track = []
        current = 0
        mid = distance * 0.6
        t = 0.2

        while current < distance:
            if current < mid:
                # 加速
                a = random.uniform(2.0, 3.0)
            else:
                # 减速
                a = -random.uniform(3.0, 5.0)

            v = 0 if not track else track[-1][0] / t
            move = v * t + 0.5 * a * t * t
            move = max(1, round(move))

            if current + move > distance:
                move = distance - current

            current += move

            track.append((
                move,
                random.randint(-2, 2),  # y 轴抖动
                random.uniform(0.01, 0.03)  # 延迟
            ))

        return track

    # 滑块拖动主函数
    async def drag_slider_human_like(self, page, distance):
        """
        模拟人类拖动滑块（腾讯验证码可用）
        distance: 云码返回的像素距离
        """

        slider = page.locator(".tencent-captcha-dy__slider-block")
        await slider.wait_for(timeout=10_000)

        box = await slider.bounding_box()
        if not box:
            raise RuntimeError("无法获取滑块 bounding box")

        start_x = box["x"] + box["width"] / 2
        start_y = box["y"] + box["height"] / 2

        # 实际拖动距离修正（经验值）
        target_distance = distance + random.randint(5, 10)

        self.logger.info(f"滑块拖动距离: {target_distance}px")

        await page.mouse.move(start_x, start_y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
        await page.mouse.down()

        # 生成人类轨迹
        track = self._generate_slider_track(target_distance)

        current_x = start_x
        for dx, dy, delay in track:
            current_x += dx
            await page.mouse.move(current_x, start_y + dy)
            await asyncio.sleep(delay)

        # 停顿 + 微调回拉
        await asyncio.sleep(random.uniform(0.05, 0.15))
        await page.mouse.move(current_x - random.randint(1, 3), start_y)
        await asyncio.sleep(random.uniform(0.02, 0.05))

        await page.mouse.up()

