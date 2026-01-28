from core.browser import BrowserManager
from core.login import LingXingERPLogin
from moudles.amazon_filter_automation import AmazonFilterAutomation
import asyncio
from moudles.amazon_Restock_Notice import NotificationManager


async def main():
    """主函数 - 修改版：实时处理通知"""
    # 创建浏览器管理器
    browser_manager = BrowserManager(headless=False)

    # 初始化通知管理器（稍后会在登录后设置logger）
    notification_manager = None

    try:
        # 启动浏览器
        page = await browser_manager.start(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1366, "height": 768}
        )

        # 创建登录实例
        client = LingXingERPLogin('Amazon_Stock_Watcher', page)
        await client.login()

        # =================数据爬取==================
        offer_filter = AmazonFilterAutomation(page, client.logger)

        # 初始化通知管理器（使用客户端的logger）
        notification_manager = NotificationManager(client.logger)

        # 1.条件筛选（不监听）
        await offer_filter.get_offer_filter()

        all_items = []
        # 2.点击确认（监听）
        items = await offer_filter.do_confirm()
        if items:
            all_items.extend(items)

        # 3.循环翻页并实时处理
        while not offer_filter.should_stop:
            next_items = await offer_filter.next_page()
            if not next_items:
                break

            all_items.extend(next_items)

            # 可选：添加延迟避免请求过快
            await asyncio.sleep(1)

        # 4.处理数据并发送通知
        stockout_msgs, fba_msgs = notification_manager.process_items_for_notifications(all_items)

        # 发送断货通知
        notification_manager.send_to_wechat_group(stockout_msgs, "断货")

        # 发送FBA预警通知
        notification_manager.send_to_wechat_group(fba_msgs, "FBA预警")

        # 5.保存所有数据到文件
        if all_items:
            # offer_filter.save_batch(all_items)
            client.logger.info(f"\n总共爬取 {len(all_items)} 条数据")
        else:
            client.logger.warning("未爬取到任何数据")

    except Exception as e:
        if notification_manager:
            notification_manager.logger.error(f"程序运行出错: {e}")
        else:
            print(f"程序运行出错: {e}")
        raise
    finally:
        # 确保浏览器关闭
        await browser_manager.close()

if __name__ == "__main__":
    asyncio.run(main())