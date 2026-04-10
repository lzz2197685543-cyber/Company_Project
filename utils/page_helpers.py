# util/page_helpers.py

from playwright.async_api import Page


async def close_popup_if_exists(page: Page):
    """
    关闭典雷达页面可能出现的新手引导 / 弹窗
    不存在则直接跳过
    """
    try:
        close_btn = page.locator(".el-dialog__body .close-btn")
        if await close_btn.count() > 0:
            await close_btn.first.wait_for(state="visible", timeout=3000)
            await close_btn.first.click()
            print("❎ 已关闭引导弹窗2")
            await page.wait_for_timeout(500)
    except Exception:
        pass

async def close_btn_if_exists(page:Page):
    try:
        close_btn = page.locator("#__layout > div > div:nth-child(19) > div > div.el-dialog__body > div > div.close")
        if await close_btn.count() > 0:
            await close_btn.first.wait_for(state="visible", timeout=3000)
            await close_btn.first.click()
            print("❎ 已关闭引导弹窗1")
            await page.wait_for_timeout(500)
    except Exception:
        pass

async def temu_close_popup_if_exists(page: Page):
    try:
        close_btn = page.locator(".arco-icon.arco-icon-close.close-icon")
        # print('检查到的弹窗个数:', close_btn)
        if await close_btn.count() > 0:
            await close_btn.first.wait_for(state="visible", timeout=3000)
            await close_btn.first.click()
            print("❎ 已关闭引导弹窗")
            await page.wait_for_timeout(500)
    except Exception:
        pass

## temu---极鲸云 查询处的一个弹框
async def temu_close1_popup_if_exists(page: Page):
    try:
        close_btn = page.locator(".arco-icon.arco-icon-close.close-icon")
        # print('检查到的弹窗个数:', close_btn)
        if await close_btn.count() > 0:
            await close_btn.first.wait_for(state="visible", timeout=3000)
            await close_btn.first.click()
            print("❎ 已关闭引导弹窗")
            await page.wait_for_timeout(500)
    except Exception:
        pass

# 检测框
async def check_if_exists(page: Page):
    try:
        print('检测框')
        check=await page.locator('#instructionText').inner_text()
        print(check)
        check_btn=page.locator("#verifyCheckbox")
        if await check_btn.count() > 0:
            await check_btn.first.wait_for(state="visible", timeout=3000)
            await check_btn.first.click()
            print('检测到检测框')
    except Exception:
        pass

