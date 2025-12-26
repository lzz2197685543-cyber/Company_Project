# utils/page_helpers.py

from playwright.async_api import Page


async def close_popup_if_exists(page: Page):
    """
    关闭典雷达页面可能出现的新手引导 / 弹窗
    不存在则直接跳过
    """
    try:
        close_btn = page.locator(".close-btn")
        if await close_btn.count() > 0:
            await close_btn.first.wait_for(state="visible", timeout=3000)
            await close_btn.first.click()
            print("❎ 已关闭引导弹窗")
            await page.wait_for_timeout(500)
    except Exception:
        pass
