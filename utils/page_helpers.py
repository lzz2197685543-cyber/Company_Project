# util/page_helpers.py

from playwright.async_api import Page
from utils.dingtalk_bot import ding_bot_send
import asyncio


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


async def handle_security_verification(page):
    """处理安全验证"""
    # 等待页面稳定
    try:
        await page.wait_for_load_state("networkidle", timeout=5000)
    except:
        pass

    # 再等一小段时间确保渲染完成
    # await asyncio.sleep(1)

    # 检查是否出现验证页面
    try:
        page_content = await page.content()
    except:
        # 如果获取失败，短暂等待后重试
        page_content = await page.content()

    if "Security Verification" in page_content or "Tencent Cloud EdgeOne" in page_content:
        # 发送钉钉通知
        ding_bot_send('me',
                      "⚠️ 检测到安全验证页面\n"
                      f"标题: Security Verification\n"
                      f"需要手动完成验证或等待自动验证"
                      )

        # 方案A：等待用户手动验证（设置较长时间）
        print("检测到安全验证，请在浏览器中手动完成验证...")

        # 等待验证完成（检查验证是否消失）
        await wait_for_verification_complete(page)


async def wait_for_verification_complete(page, timeout=None):
    """等待验证完成，默认无限等待直到完成"""
    start_time = asyncio.get_event_loop().time()

    print("等待安全验证完成...")
    print("🔐 检测到安全验证，请手动完成验证")

    while True:
        try:
            # 等待页面稳定后再获取内容
            await page.wait_for_load_state("networkidle", timeout=5000)

            # 检查验证页面是否消失
            current_content = await page.content()

            if "Security Verification" not in current_content and "Tencent Cloud EdgeOne" not in current_content:
                elapsed = asyncio.get_event_loop().time() - start_time
                print(f"验证通过，继续执行... (等待了 {elapsed:.1f} 秒)")
                break

        except Exception as e:
            # 如果页面还在导航中，短暂等待后继续
            if "navigating" in str(e):
                await asyncio.sleep(1)
                continue
            else:
                # 其他异常记录但继续等待
                print(f"检查页面时出现异常: {e}")
                await asyncio.sleep(0.5)
                continue

        # 可选：每隔一段时间发送提醒
        elapsed = asyncio.get_event_loop().time() - start_time
        if int(elapsed) % 30 == 0 and int(elapsed) > 0:  # 每30秒提醒一次
            ding_bot_send('me', f"⏳ 仍在等待验证... 已等待 {int(elapsed)} 秒")
            print(f"⏳ 仍在等待验证... 已等待 {int(elapsed)} 秒")

        await asyncio.sleep(2)  # 每2秒检查一次
