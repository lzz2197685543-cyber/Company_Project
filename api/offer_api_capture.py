# api/offer_api_capture.py
import random
from datetime import datetime
from utils.dingtalk_bot import ding_bot_send
import asyncio

class OfferApiCapture:
    API_KEYWORD = "dld/api/offerSearch/queryList"
    MONTH_ORDER_API = "dld/api/alibaba/getOfferVideoByTypeThreeSecondsReturn"
    COLLECT_CUSTOMER_API = "dld/api/alibaba/getOfferVideoByType/5"
    COMMENT_API = "dld/api/alibaba/getOfferVideoByType/2"
    SKU_API = "dld/api/alibaba/getOfferVideoByType/1"

    def __init__(self, page, logger):
        self.page = page
        self.logger = logger
        self.timeout_offers = []  # 超时商品列表

    # 1 获取商品列表
    async def capture_once(self, monitor_id=None):
        self.logger.info("⏳ 等待点击【开始查询】并捕获 offerSearch 接口...")

        try:
            async with self.page.expect_response(
                    lambda r: self.API_KEYWORD in r.url,
                    timeout=30000
            ) as resp_info:
                await self.page.click('button.dld-button.primary:has-text("开始查询")')

            resp = await resp_info.value
            data = await resp.json()

            self.logger.info(f"商品列表返回码: {data.get('code')}")

            # 检查是否返回200但列表为空
            if data.get('code') == 200:
                result = data.get('result', {})
                data_list = result.get('list', [])

                if not data_list:
                    self.logger.warning(f"⚠️ 商品列表为空，monitor_id: {monitor_id}")

                    # 发消息到钉钉群中
                    message = f"⚠️ 监控ID [{monitor_id}] 在平台未找到任何商品,链接：https://detail.1688.com/offer/{monitor_id}.html"
                    ding_bot_send('me', message)

                    return []

            return data

        except Exception as e:
            error_msg = f"❌ 捕获商品列表接口失败: {e}"
            self.logger.error(error_msg)

            # 新增：记录超时的商品ID
            if "Timeout" in str(e) and monitor_id:
                self.timeout_offers.append({
                    'offer_id': monitor_id,
                    'url': f'https://detail.1688.com/offer/{monitor_id}.html',
                    'error': str(e),
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                self.logger.warning(f"⏰ 商品ID {monitor_id} 请求超时，已记录到超时列表")

            return {}

    # 获取超时商品列表
    def get_timeout_offers(self):
        return self.timeout_offers

    # 清空超时商品列表
    def clear_timeout_offers(self):
        self.timeout_offers = []

    # 2 解析商品列表
    async def parse(self, data):
        items = []

        try:
            if not isinstance(data, dict):
                self.logger.error("❌ 商品列表数据不是字典格式")
                return items

            data_list = data.get('result', {}).get('list', [])
            if not data_list:
                self.logger.warning("⚠️ 商品列表为空")
                return items

        except Exception as e:
            self.logger.error(f"❌ 商品列表结构异常: {e}")
            return items

        for i in data_list:
            try:
                publish_time = i.get('offerCreateTime')
                if publish_time:
                    try:
                        # 先转换为秒级时间戳，再乘以1000转为毫秒
                        timestamp_sec = datetime.fromisoformat(
                            publish_time.replace('T', ' ')
                        ).timestamp()
                        publish_time = int(timestamp_sec * 1000)  # ← 转为毫秒
                    except:
                        publish_time = None
                else:
                    publish_time = None

                repurchase = i.get('offerRepurchaseRate')
                if repurchase:
                    try:
                        repurchase = float(repurchase.rstrip('%')) / 100
                    except:
                        repurchase = 0
                else:
                    repurchase = 0

                sku_count = i.get('skuCount', 0)
                if sku_count==0:
                    sku_count = 1

                items.append({
                    "url": f'https://detail.1688.com/offer/{i.get("offerId")}.html',
                    "offerid": i.get("offerId"),
                    "publish_time": publish_time,
                    "category": i.get('levelName'),
                    "repurchase_rate": repurchase,
                    "day_order_count": i.get('dayBookedCount', 0),
                    "day_sale_quantity": i.get('daySaleQuantity', 0),
                    "day_sales_volume": i.get('daySalesVolume', 0),
                    "order_30d": i.get('bookedCount30d', 0),
                    "sales_volume_30d": i.get('salesVolume30d', 0),
                    "sale_quantity_30d": i.get('saleQuantity30d', 0),
                    "sku_count": sku_count,
                    "total_order_count": i.get('bookedCount', 0),
                    "total_sale_count": i.get('saleCount', 0),
                    "shop_name": i.get('company'),
                    "bookedCount7dGrowthRate": float(i.get('bookedCount7dGrowthRate', 0)) / 100 * i.get('bookedCount7dGrowthType', 1),
                    "category_changed": "否",
                    "count_changed": "否"
                })

            except Exception as e:
                self.logger.error(f"❌ 商品解析失败: {e}")

        return items

    # 3 获取商品基本信息（不获取SKU）
    async def capture_without_sku(self, good_ids_str):
        """
        只获取商品基本信息，不获取SKU
        good_ids_str: 用顿号分隔的商品ID字符串，如 "123、456、789"
        """
        self.logger.info(f'开始获取商品:{good_ids_str}的基本信息')
        list_data = await self.capture_once(good_ids_str)

        if not list_data:
            return [], [], self.timeout_offers

        items = await self.parse(list_data)
        total_count = len(items)

        if total_count == 0:
            return [], [], self.timeout_offers

        # 记录开始时间
        start_time = datetime.now()
        self.logger.info(f"🚀 开始捕获商品数据，共 {total_count} 个商品")

        # 总体统计
        total_elapsed = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"✅ 商品基本信息捕获完成，总耗时: {total_elapsed:.2f}秒")
        self.logger.info(f"   - 商品数: {len(items)}")

        # 返回商品列表和空SKU列表
        return items, [], self.timeout_offers

    # 4 获取SKU信息（使用独立的 DrissionPage 浏览器）
    async def capture_sku_only(self, url_list):
        """单独获取SKU信息（使用 DrissionPage，独立浏览器）"""
        from api.sku_from_plugin import SkuFromPlugin

        self.logger.info("=" * 60)
        self.logger.info("🔍 开始获取SKU信息...")
        self.logger.info(f"📦 共 {len(url_list)} 个商品需要获取SKU")
        self.logger.info("=" * 60)

        sku_fetcher = None
        all_sku_items = []

        try:
            # 创建SKU获取器
            sku_fetcher = SkuFromPlugin(headless=False, job="ali1688_goods_monitor")

            # 初始化浏览器
            self.logger.info("🚀 初始化SKU获取浏览器...")
            sku_fetcher.init_browser()

            # 登录店雷达
            self.logger.info("🔐 登录店雷达...")
            sku_fetcher.check_and_login()

            # 逐个获取SKU
            for idx, url in enumerate(url_list, 1):
                self.logger.info(f"🔍 [{idx}/{len(url_list)}] 获取SKU: {url}")

                try:
                    # 获取SKU数据
                    sku_items = sku_fetcher.goto_goods(url)

                    if sku_items:
                        all_sku_items.extend(sku_items)
                        self.logger.info(f"   ✅ 获取到 {len(sku_items)} 个SKU")
                    else:
                        self.logger.warning(f"   ⚠️ 未获取到SKU数据")

                except Exception as e:
                    self.logger.error(f"   ❌ 获取SKU失败: {e}")

                # 随机延迟，避免请求过快
                if idx < len(url_list):
                    delay = random.uniform(1, 2.5)
                    self.logger.info(f"   ⏳ 等待 {delay:.1f} 秒后继续...")
                    await asyncio.sleep(delay)

            self.logger.info("=" * 60)
            self.logger.info(f"✅ SKU获取完成，共获取 {len(all_sku_items)} 个SKU")
            self.logger.info("=" * 60)

            return all_sku_items

        except Exception as e:
            self.logger.error(f"❌ SKU获取失败: {e}")
            raise

        finally:
            if sku_fetcher:
                self.logger.info("🔚 关闭SKU获取浏览器...")
                sku_fetcher.close()