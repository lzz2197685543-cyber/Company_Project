from core.browser import BrowserManager
from core.new_temu_login import GeekBILogin
from core.miaoshou_login import MiaoShouLogin
import asyncio
from utils.logger import get_logger
from storage.product_dao import ProductDAO
from modules.miaoshou.publish import AutoPublish
from config.settings import SHOP_IDS, SHOP_DAILY_LIMIT


def chunk_shops(size=3):
    return [SHOP_IDS[i:i + size] for i in range(0, len(SHOP_IDS), size)]


async def collect(limit, logger, page):
    # =================点击一键上架，并且点打开采集箱==================
    tasks = ProductDAO.fetch_pending(limit=limit)
    goods_ids = [t["goods_id"] for t in tasks]
    if not tasks:
        logger.info('没有可处理任务')
        return

    for task in tasks:
        goods_id = task["goods_id"]

        try:
            await page.goto(f'https://www.geekbi.com/data/goods/detail?goodsId={goods_id}&siteId=48')

            await asyncio.sleep(1)

            # 点击“一键上架”
            await page.locator(
                "#app > div > main > div.right > div > div:nth-child(3) > div.arco-card-body > div > div:nth-child(2) > div.arco-space.arco-space-horizontal.arco-space-align-center.btn-wrap > div:nth-child(6) > button").click()

            # 点击"同意并继续"
            # 尝试点击"同意并继续"（如果存在且可见）
            try:
                confirm_btn = page.locator(
                    'body > div:nth-child(5) > div.arco-modal-wrapper.arco-modal-wrapper-align-center > div > div.arco-modal-footer > button.arco-btn.arco-btn-primary.arco-btn-shape-square.arco-btn-size-medium.arco-btn-status-normal')
                if await confirm_btn.count() > 0 and await confirm_btn.is_visible():
                    await confirm_btn.click()
                    logger.info("点击了同意按钮")
                    await asyncio.sleep(0.5)
                else:
                    logger.info("同意按钮不存在或不可见，跳过点击")
            except Exception as e:
                logger.info(f"同意按钮处理跳过: {e}")

            # 获取浏览器上下文（如果还没有获取）
            context = page.context

            # 准备捕获新页面
            async with context.expect_page() as new_page_info:
                # 点击打开采集箱
                collect_btn = page.locator(
                    'body > div.arco-modal-container.one-click-shelf-modal > div.arco-modal-wrapper.arco-modal-wrapper-align-center > div > div.arco-modal-body > div > div.loading-section > div:nth-child(2) > button'
                )
                await collect_btn.click()
                logger.info("已点击打开采集箱，等待新页面打开")

            # 获取新页面对象
            new_page = await new_page_info.value
            logger.info("新页面（采集箱）已打开")

            try:
                # 等待"已提交采集任务"这个文本出现（最多等待10秒，可调整）
                await new_page.wait_for_selector('text=已提交采集任务', timeout=10000)
                logger.info('检测到“已提交采集任务”，关闭采集箱页面')
                await new_page.close()
                logger.info('采集箱新页面已自动关闭')

                # ✅ 标记成功
                ProductDAO.update_status(goods_id, "collected")  # collected表示采集完成
                logger.info(f"✅ 上架成功: {goods_id}")

            except Exception as e:
                # 如果10秒内没有检测到该文本，根据需求处理
                logger.warning('未检测到“已提交采集任务”，继续等待一段时间后关闭')
                await asyncio.sleep(1)
                await new_page.close()
                logger.info('采集箱新页面已关闭（超时后）')

                # ✅ 标记失败（自动重试）
                ProductDAO.mark_failed(goods_id)

        except Exception as e:
            logger.error(f"❌ 上架失败: {goods_id} - {e}")

    await asyncio.sleep(2)

    return goods_ids


async def main():
    s = AutoPublish('auto_listing')

    job = 'auto_listing'
    """主函数 - 使用方式1：手动管理浏览器"""
    # 创建浏览器管理器
    browser_manager = BrowserManager(headless=False)
    logger = get_logger(job)

    try:
        # 启动浏览器
        page = await browser_manager.start(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1366, "height": 768}
        )

        # ================先登录妙手app========
        miaoshou = MiaoShouLogin(page, job)
        await miaoshou.login()

        # ================创建登录实例================
        client = GeekBILogin(page, job)
        await client.login()

        shop_groups = chunk_shops(size=3)
        for group in shop_groups:
            logger.info('开始上架商品到---{}'.format(group))
            # 采集数据到妙手中
            goods_ids = await collect(SHOP_DAILY_LIMIT, logger, page)

            # 将数据上传
            await s.dispatch_publish(group, goods_ids)

    finally:
        # 关闭浏览器
        await browser_manager.close()


if __name__ == '__main__':
    asyncio.run(main())
