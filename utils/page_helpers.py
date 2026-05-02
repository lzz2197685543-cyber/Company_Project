from playwright.async_api import Page

async def next_btn(page:Page):
    try:
        close_btn = page.locator("#auto-height > div > div.el-dialog__body > span > button")
        if await close_btn.count() > 0:
            await close_btn.first.wait_for(state="visible", timeout=3000)
            await close_btn.first.click()
            print("已经点击下一条")
            await page.wait_for_timeout(500)
    except Exception:
        pass


async def close_btn(page:Page):
    try:
        close_btn = page.locator("#auto-height > div > div.el-dialog__body > span > button.el-button.new-verion-btn.ak-width-84.el-button--primary.el-button--medium.is-round")
        if await close_btn.count() > 0:
            await close_btn.first.wait_for(state="visible", timeout=3000)
            await close_btn.first.click()
            print("已经点击关闭")
            await page.wait_for_timeout(500)
    except Exception:
        pass