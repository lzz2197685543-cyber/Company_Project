# collect_and_publish.py
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

from core.browser import BrowserManager
from core.new_temu_login import GeekBILogin
from core.miaoshou_login import MiaoShouLogin
from modules.miaoshou.publish import AutoPublish
from storage.product_dao import ProductDAO
from config.settings import SHOP_GROUPS, SHOP_DAILY_LIMIT
from utils.logger import get_logger
from utils.page_helpers import temu_close_popup_if_exists,handle_security_verification,wait_for_verification_complete


job = "auto_listing"
product_dao = ProductDAO(job)

# 如果你的 collect_and_publish.py 就在项目根目录，这样写是对的
PLAN_FILE = Path(__file__).resolve().parent / "data" / "tmp" / "group_goods_plan.json"
PLAN_FILE.parent.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 2
RETRY_DELAY_BASE = 2


def format_seconds(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}分{s}秒"


def load_plan():
    if not PLAN_FILE.exists():
        return {}
    try:
        with open(PLAN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_plan(plan: dict):
    with open(PLAN_FILE, "w", encoding="utf-8") as f:
        json.dump(plan, f, ensure_ascii=False, indent=2)

async def collect_with_retry(goods_id, page, logger, max_retries=MAX_RETRIES):
    """带重试机制的单个商品采集"""
    context = page.context

    for attempt in range(max_retries):
        new_page = None
        try:
            logger.info(f"商品 {goods_id} 第 {attempt + 1}/{max_retries} 次采集尝试")

            await page.goto(
                f"https://www.geekbi.com/data/goods/detail?goodsId={goods_id}&siteId=48",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await handle_security_verification(page)
            await page.wait_for_timeout(1000)

            # 点击“一键上架”
            await page.locator(
                "#app > div > main > div.right > div > div:nth-child(3) > div.arco-card-body > div > div:nth-child(2) > div.arco-space.arco-space-horizontal.arco-space-align-center.btn-wrap > div:nth-child(6) > button"
            ).click()

            # 点击“同意并继续”
            try:
                confirm_btn = page.locator(
                    "body > div:nth-child(5) > div.arco-modal-wrapper.arco-modal-wrapper-align-center > div > div.arco-modal-footer > button.arco-btn.arco-btn-primary.arco-btn-shape-square.arco-btn-size-medium.arco-btn-status-normal"
                )
                if await confirm_btn.count() > 0 and await confirm_btn.is_visible():
                    await confirm_btn.click()
                    logger.info(f"点击了同意按钮 - {goods_id}")
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.info(f"同意按钮处理跳过: {e}")

            # 打开采集箱新页面
            async with context.expect_page(timeout=15000) as new_page_info:
                collect_btn = page.locator(
                    "body > div.arco-modal-container.one-click-shelf-modal > div.arco-modal-wrapper.arco-modal-wrapper-align-center > div > div.arco-modal-body > div > div.loading-section > div:nth-child(2) > button"
                )
                await collect_btn.click()
                logger.info(f"已点击打开采集箱，等待新页面打开 - {goods_id}")

            new_page = await new_page_info.value
            logger.info(f"新页面（采集箱）已打开 - {goods_id}")

            success_state = False
            try:
                # 同时等待两个条件，任意一个完成即可
                done, pending = await asyncio.wait(
                    [
                        new_page.wait_for_selector('text=已提交采集任务', timeout=5000),
                        new_page.wait_for_selector('text=公用采集箱', timeout=5000)
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=10000
                )

                # 取消未完成的任务
                for task in pending:
                    task.cancel()

                if done:
                    logger.info(f'检测到成功状态（已提交/公共采集箱） - {goods_id}')
                    success_state = True
                else:
                    raise asyncio.TimeoutError()

            except asyncio.TimeoutError:
                logger.error("未检测到成功状态（已提交采集任务 / 公共采集箱）")
            except Exception as e:
                logger.error(f"检测过程中出错: {e}")

            await asyncio.sleep(1.5)

            if success_state:
                product_dao.update_status(goods_id, "collected")
                logger.info(f"✅ 上架成功: {goods_id}")
                return True

        except Exception as e:
            logger.warning(f"商品 {goods_id} 采集失败 (尝试 {attempt + 1}/{max_retries}): {e}")

            if attempt == max_retries - 1:
                product_dao.update_status(goods_id, "failed")
                return False

            try:
                if page and not page.is_closed():
                    await page.reload(wait_until="domcontentloaded", timeout=30000)
            except Exception as reload_err:
                logger.warning(f"商品 {goods_id} 重试前刷新失败: {reload_err}")

            await asyncio.sleep(RETRY_DELAY_BASE * (2 ** attempt))

        finally:
            try:
                if new_page and not new_page.is_closed():
                    await new_page.close()
                    logger.info(f"采集箱新页面已关闭 - {goods_id}")
            except Exception as e:
                logger.warning(f"关闭采集箱页面失败 - {goods_id}: {e}")

async def collect_concurrent(limit, logger, main_page):
    """批量采集商品，带并发控制和重试机制"""
    product_dao.recover_stale_processing(timeout_minutes=30)

    tasks = product_dao.fetch_pending(limit=limit)
    if not tasks:
        logger.info("没有可处理任务")
        return []

    logger.info(f"开始处理 {len(tasks)} 个商品的采集任务")
    context = main_page.context
    semaphore = asyncio.Semaphore(2)

    active_pages = {}
    active_lock = asyncio.Lock()

    async def close_orphan_pages():
        """关闭不是 main_page，也不属于当前活跃 worker 的页面"""
        async with active_lock:
            keep_ids = {id(main_page)} | set(active_pages.keys())

        for p in list(context.pages):
            try:
                if id(p) in keep_ids:
                    continue
                if not p.is_closed():
                    await p.close()
                    logger.info("关闭采集后残留页面")
            except Exception as e:
                logger.warning(f"关闭残留页面失败: {e}")

    async def process_one(goods_id, idx, total):
        async with semaphore:
            page = await context.new_page()
            async with active_lock:
                active_pages[id(page)] = page

            try:
                logger.info(f"进度: {idx}/{total} - 正在处理商品: {goods_id}")
                success = await asyncio.wait_for(
                    collect_with_retry(goods_id, page, logger),
                    timeout=180,
                )
                return success

            except asyncio.TimeoutError:
                logger.error(f"商品 {goods_id} 处理超时")
                return False
            except Exception as e:
                logger.error(f"商品 {goods_id} 任务异常: {e}")
                return False
            finally:
                try:
                    if not page.is_closed():
                        await page.close()
                        logger.info(f"外层采集页面已关闭 - {goods_id}")
                except Exception as e:
                    logger.warning(f"关闭外层页面失败 - {goods_id}: {e}")

                async with active_lock:
                    active_pages.pop(id(page), None)

                await close_orphan_pages()

    start_time = time.perf_counter()
    tasks_coro = [process_one(t["goods_id"], i, len(tasks)) for i, t in enumerate(tasks, 1)]
    results = await asyncio.gather(*tasks_coro, return_exceptions=True)

    success_count = 0
    failed_count = 0
    for idx, r in enumerate(results):
        if isinstance(r, Exception):
            failed_count += 1
            logger.error(f"商品 {tasks[idx]['goods_id']} 任务异常: {r}")
        elif r is True:
            success_count += 1
        else:
            failed_count += 1

    elapsed_time = time.perf_counter() - start_time
    logger.info(
        f"采集任务完成 - 成功: {success_count}, 失败: {failed_count}, 总耗时: {format_seconds(elapsed_time)}"
    )

    successful_goods_ids = []
    for idx, r in enumerate(results):
        if r is True:
            successful_goods_ids.append(tasks[idx]["goods_id"])

    return successful_goods_ids

async def main():
    total_start = time.perf_counter()
    logger = get_logger(job)
    browser_manager = BrowserManager(headless=False)

    try:
        await browser_manager.start(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1366, "height": 768},
        )
        page = browser_manager.page
        main_page = page

        # 登录妙手
        miaoshou = MiaoShouLogin(page, job)
        ok = await miaoshou.login()
        if not ok:
            logger.error("妙手登录失败，本次任务结束")
            return

        # 登录 GeekBI
        client = GeekBILogin(page, job)
        await client.login()

        # 关闭登录后残留页面
        for p in list(page.context.pages):
            if p != main_page:
                try:
                    if not p.is_closed():
                        await p.close()
                        logger.info("关闭登录残留页面")
                except Exception as e:
                    logger.warning(f"关闭登录残留页面失败: {e}")

        publisher = AutoPublish(job)

        # 更新保存：同一个 idx 会被覆盖
        plan = load_plan()

        # 按固定店铺组逐组处理：采集一组 -> 上架一组 -> 再采下一组
        for idx, group in enumerate(SHOP_GROUPS, start=1):
            logger.info(f"开始处理第 {idx} 组店铺: {group}")

            goods_ids = await collect_concurrent(SHOP_DAILY_LIMIT, logger, page)

            plan[str(idx)] = {
                "group": group,
                "goods_ids": goods_ids,
            }
            save_plan(plan)

            # 清理采集后残留页面
            for p in list(page.context.pages):
                if p != main_page:
                    try:
                        if not p.is_closed():
                            await p.close()
                            logger.info("关闭采集后残留页面")
                    except Exception as e:
                        logger.warning(f"关闭采集后残留页面失败: {e}")

            if not goods_ids:
                logger.info(f"第 {idx} 组没有需要发布的商品，跳过")
                continue

            batch_id = datetime.now().strftime("%Y%m%d%H%M%S") + f"_{idx}"
            logger.info(f"第 {idx} 组采集完成，准备发布，商品数: {len(goods_ids)}，批次: {batch_id}")

            await asyncio.sleep(20)

            all_items = await publisher.get_detial_id()

            await publisher.dispatch_publish_concurrent(
                group=group,
                target_goods_ids=goods_ids,
                items=all_items,
                batch_id=batch_id,
                max_concurrent=3,
            )

            # 清理发布后残留页面
            for p in list(page.context.pages):
                if p != main_page:
                    try:
                        if not p.is_closed():
                            await p.close()
                            logger.info("关闭发布后残留页面")
                    except Exception as e:
                        logger.warning(f"关闭发布后残留页面失败: {e}")


        logger.info("主流程执行完毕，开始运行 full_process 补漏")
        await publisher.full_process()

    finally:
        total_cost = time.perf_counter() - total_start
        logger.info(f"🎯 全流程完成，总耗时：{format_seconds(total_cost)}")
        await browser_manager.close()


if __name__ == "__main__":
    asyncio.run(main())