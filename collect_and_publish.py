from core.browser import BrowserManager
from core.new_temu_login import GeekBILogin
from core.miaoshou_login import MiaoShouLogin
from utils.logger import get_logger
from storage.product_dao import ProductDAO
from modules.miaoshou.publish import AutoPublish
from config.settings import SHOP_IDS, SHOP_DAILY_LIMIT
from utils.dingtalk_bot import ding_bot_send
import asyncio
import time

job = 'auto_listing'

product_dao = ProductDAO(job)

def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"


def chunk_shops(size=3):
    return [SHOP_IDS[i:i + size] for i in range(0, len(SHOP_IDS), size)]


def chunk_list(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]



# 在文件开头添加重试相关的常量
MAX_RETRIES = 2
RETRY_DELAY_BASE = 2  # 基础延迟时间（秒）


# 修改 collect_with_retry 函数
async def collect_with_retry(goods_id, page, logger, max_retries=MAX_RETRIES):
    """带重试机制的单个商品采集"""
    for attempt in range(max_retries):
        try:
            await page.goto(f'https://www.geekbi.com/data/goods/detail?goodsId={goods_id}&siteId=48')
            await asyncio.sleep(1)

            # 点击"一键上架"
            await page.locator(
                "#app > div > main > div.right > div > div:nth-child(3) > div.arco-card-body > div > div:nth-child(2) > div.arco-space.arco-space-horizontal.arco-space-align-center.btn-wrap > div:nth-child(6) > button"
            ).click()

            # 点击"同意并继续"
            try:
                confirm_btn = page.locator(
                    'body > div:nth-child(5) > div.arco-modal-wrapper.arco-modal-wrapper-align-center > div > div.arco-modal-footer > button.arco-btn.arco-btn-primary.arco-btn-shape-square.arco-btn-size-medium.arco-btn-status-normal'
                )
                if await confirm_btn.count() > 0 and await confirm_btn.is_visible():
                    await confirm_btn.click()
                    logger.info(f"点击了同意按钮 - {goods_id}")
                    await asyncio.sleep(0.5)
                else:
                    logger.info(f"同意按钮不存在或不可见，跳过点击 - {goods_id}")
            except Exception as e:
                logger.info(f"同意按钮处理跳过: {e}")

            # 获取浏览器上下文
            context = page.context

            # 准备捕获新页面
            async with context.expect_page() as new_page_info:
                # 点击打开采集箱
                collect_btn = page.locator(
                    'body > div.arco-modal-container.one-click-shelf-modal > div.arco-modal-wrapper.arco-modal-wrapper-align-center > div > div.arco-modal-body > div > div.loading-section > div:nth-child(2) > button'
                )
                await collect_btn.click()
                logger.info(f"已点击打开采集箱，等待新页面打开 - {goods_id}")

            # 获取新页面对象
            new_page = await new_page_info.value
            logger.info(f"新页面（采集箱）已打开 - {goods_id}")

            try:
                # 等待"已提交采集任务"文本出现
                await new_page.wait_for_selector('text=已提交采集任务', timeout=10000)
                logger.info(f'检测到"已提交采集任务"，关闭采集箱页面 - {goods_id}')
                await new_page.close()
                logger.info(f'采集箱新页面已自动关闭 - {goods_id}')

                # ✅ 使用实例方法
                product_dao.update_status(goods_id, "collected")
                logger.info(f"✅ 上架成功: {goods_id}")
                return True  # 成功返回

            except Exception as e:
                # 超时处理
                logger.warning(f'等待采集任务提交超时 - {goods_id}')
                await new_page.close()
                logger.info(f'采集箱新页面已关闭（超时后） - {goods_id}')

                # 抛出异常以便重试
                raise Exception(f"等待采集任务提交超时: {e}")

        except Exception as e:
            error_msg = str(e)

            # 判断是否需要重试
            if attempt < max_retries - 1:
                # 指数退避策略
                delay = RETRY_DELAY_BASE * (2 ** attempt)
                logger.warning(
                    f"⚠️ 商品 {goods_id} 采集失败，第 {attempt + 1}/{max_retries} 次重试，等待 {delay} 秒 - 错误: {error_msg[:100]}")
                await asyncio.sleep(delay)

                # 如果是网络错误，可以考虑刷新页面或重新登录
                if "net::" in error_msg or "timeout" in error_msg.lower():
                    try:
                        await page.reload()
                        await asyncio.sleep(2)
                    except:
                        pass
            else:
                # 所有重试都失败
                logger.error(f"❌ 商品 {goods_id} 采集失败，已重试 {max_retries} 次 - 错误: {error_msg}")
                product_dao.update_status('fail',goods_id)  # ✅ 使用实例方法
                return False

    return False


async def collect(limit, logger, page):
    """批量采集商品，带重试机制"""
    # 获取待处理任务
    tasks = product_dao.fetch_pending(limit=limit)

    if not tasks:
        logger.info('没有可处理任务')
        return []

    logger.info(f'开始处理 {len(tasks)} 个商品的采集任务')

    goods_ids = [t["goods_id"] for t in tasks]

    # 统计信息
    success_count = 0
    failed_count = 0
    start_time = time.perf_counter()

    # 逐个处理，带重试
    for idx, task in enumerate(tasks, 1):
        goods_id = task["goods_id"]
        logger.info(f'进度: {idx}/{len(tasks)} - 正在处理商品: {goods_id}')

        # 执行采集（带重试）
        success = await collect_with_retry(goods_id, page, logger)

        if success:
            success_count += 1
        else:
            failed_count += 1

        # 每个商品之间添加短暂延迟，避免请求过快
        if idx < len(tasks):
            await asyncio.sleep(1)

    # 统计总耗时
    elapsed_time = time.perf_counter() - start_time
    logger.info(
        f'采集任务完成 - 成功: {success_count}, 失败: {failed_count}, 总耗时: {format_seconds(elapsed_time)}')

    # ✅ 使用实例方法获取成功的商品ID
    successful_goods_ids = product_dao.get_successful_goods(goods_ids)

    return successful_goods_ids


async def main():
    total_start = time.perf_counter()


    logger = get_logger(job)
    browser_manager = BrowserManager(headless=False)

    try:
        # 启动浏览器（创建主页面用于登录）
        await browser_manager.start(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1366, "height": 768}
        )
        page = browser_manager.page

        # 登录妙手
        miaoshou = MiaoShouLogin(page, job)
        await miaoshou.login()

        # 登录 GeekBI
        client = GeekBILogin(page, job)
        await client.login()

        # 将店铺 ID 列表分组成多个批次（每组最多3个店铺）
        shop_groups = chunk_shops(size=3)

        # ✅ 一次性采集全部数据
        total_limit = SHOP_DAILY_LIMIT * len(shop_groups)

        goods_ids = await collect(total_limit, logger, page)

        if not goods_ids:
            logger.info('没有需要发布的商品')
            return

        group_goods = chunk_list(goods_ids, SHOP_DAILY_LIMIT)

        s = AutoPublish('auto_listing')

        for i, group in enumerate(shop_groups):
            goods = group_goods[i] if i < len(group_goods) else []

            if not goods:
                logger.info(f'店铺组 {group} 没有分配到商品，跳过')
                continue

            logger.info(f'开始处理店铺组: {group}，商品数: {len(goods)}')

            await s.dispath_publish_concurrent(group, goods, max_concurrent=3)

    finally:
        total_cost = time.perf_counter() - total_start
        logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
        # ding_bot_send('me', f'auto_listing任务结束，总耗时：{format_seconds(total_cost)}')
        await browser_manager.close()


if __name__ == '__main__':
    asyncio.run(main())


