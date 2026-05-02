"""当在店雷达中爬取不到sku价格，那么就在这个页面爬取"""
import re
import json
from utils.logger import get_logger
from utils.cookie_manager import CookieManager
import asyncio
from utils.requestmanager import RequestManager

class AlibabaSkuFetcher:
    """
    当在店雷达中爬取不到SKU价格时，从1688详情页解析SKU
    """

    def __init__(self,request_manager: RequestManager,logger=None):
        self.request_manager = request_manager  # 注入请求管理器
        self.cookies = None
        self.logger= self.logger = logger or get_logger('ali1688_goods_monitor')
        self.headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        }
        # 使用支持池的 CookieManager
        self.cookie_manager = CookieManager('ali1688_goods_monitor', use_pool=True)

    # -------------------------
    # 请求页面
    # -------------------------
    async def fetch_page(self, url):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 1.每次请求前获取最新的cookie
                cookies = await self.cookie_manager.get_auth()
                if not cookies:
                    self.logger.warning("无可用 Cookie，尝试刷新池")
                    await self.cookie_manager.refresh()
                    cookies = await self.cookie_manager.get_auth()
                    if not cookies:
                        continue

                # 【修改】获取账号名，兼容单Cookie模式
                current_account = cookies.get('__account')

                # 如果是单Cookie模式（没有__account字段），从cookie_manager获取或使用默认值
                if not current_account:
                    # 尝试从cookie_manager获取当前账号
                    if hasattr(self.cookie_manager, 'current_account') and self.cookie_manager.current_account:
                        current_account = self.cookie_manager.current_account
                    else:
                        # 单Cookie模式，使用默认账号名
                        current_account = "ali1688"  # 默认账号
                        self.logger.debug(f"单Cookie模式，使用默认账号: {current_account}")


                self.logger.info(f"使用账号 {current_account} 请求 {url}")

                # 2.使用 RequestManager 发起请求
                response = self.request_manager.get(
                    url,
                    headers=self.headers,  # 可以传入自定义headers
                    cookies=cookies,  # 将cookie作为参数传入
                    timeout=30,
                    # 如果需要代理，可以在初始化 RequestManager 时开启 use_proxy=True
                )

                if response is None:  # RequestManager 失败时返回 None
                    print(f"请求失败（第 {attempt + 1} 次）")
                    self.cookie_manager.mark_result(cookies, success=False)
                    if attempt < max_retries - 1:
                        print("准备重试...")
                        continue
                    return None

                if response.status_code != 200:
                    print(f"状态码异常: {response.status_code}")
                    self.cookie_manager.mark_result(cookies, success=False)
                    if attempt < max_retries - 1:
                        continue
                    return None

                # 3.检查页面内容是否包含 skuModel
                if 'skuModel' not in response.text:
                    print("未找到 skuModel，刷新 cookie")
                    self.cookie_manager.mark_result(cookies, success=False)

                    if attempt < max_retries - 1:
                        # 触发刷新池（可选，也可让池自动维护）
                        await self.cookie_manager.refresh_by_account(current_account)
                        continue
                    return None

                # 4. 请求成功，标记该 Cookie 成功
                self.cookie_manager.mark_result(cookies, success=True)
                return response.text

            except Exception as e:
                print(f"请求异常: {e}")
                # 异常时也尝试标记失败（如果 cookies 存在）
                if 'cookies' in locals() and cookies:
                    self.cookie_manager.mark_result(cookies, success=False)
                if attempt < max_retries - 1:
                    continue
                return None
        return None

    # -------------------------
    # 提取JSON
    # -------------------------
    def extract_valid_json(self, data):

        start = data.find("{")
        end = data.rfind("}") + 1

        if start == -1:
            return None

        json_str = data[start:end]

        try:
            return json.loads(json_str)

        except:

            json_str = re.sub(r'//.*?\n', '\n', json_str)
            json_str = re.sub(r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3', json_str)
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)

            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                print("JSON解析失败:", e)
                return None

    # -------------------------
    # 安全float
    # -------------------------
    def safe_float(self, value):

        if value is None:
            return 0.0

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):

            try:
                value = value.replace("¥", "").replace(",", "").strip()
                return float(value) if value else 0.0

            except:
                return 0.0

        return 0.0

    # -------------------------
    # 安全int
    # -------------------------
    def safe_int(self, value):

        if value is None:
            return 0

        if isinstance(value, (int, float)):
            return int(value)

        if isinstance(value, str):

            try:
                value = value.replace(",", "").strip()
                return int(float(value)) if value else 0

            except:
                return 0

        return 0

    # -------------------------
    # 解析SKU
    # -------------------------
    def extract_sku_rows(self, json_data, url):
        sku_rows = []

        try:
            data = json_data["result"]["data"]

            # -------------------------
            # SKU详情
            # -------------------------
            root_fields = data["Root"]["fields"]
            data_json = root_fields["dataJson"]
            sku_model = data_json.get("skuModel", {})
            sku_info_map = sku_model.get("skuInfoMap", {})

            goods_id=url.split("/")[-1].replace(".html", "")

            if sku_info_map==[]:
                price_list=data['mainPrice']['fields']['priceModel']['currentPrices']

                sku_row = {
                    "url": url,
                    "skuid":goods_id+"_2",
                    "sku_name":'',
                    "page_price": price_list[0]['price'],
                    "sku_count":0,
                    "price_changed": "否",
                    "name_changed": "否",
                    "count_changed": "否",
                }
                sku_row1={
                    "url": url,
                    "skuid":goods_id+"_1",
                    "sku_name":'',
                    "page_price": price_list[-1]['price'],
                    "sku_count":0,
                    "price_changed": "否",
                    "name_changed": "否",
                    "count_changed": "否",
                }
                print(sku_row)
                print(sku_row1)
                sku_rows.append(sku_row)
                sku_rows.append(sku_row1)

            else:

                for sku_name, sku_info in sku_info_map.items():
                    price = self.safe_float(sku_info.get("discountPrice")) or data['mainPrice']['fields']['priceModel']['currentPrices'][0]['price']

                    sku_row = {
                        "url": url,
                        "skuid": sku_info.get("skuId"),
                        "sku_name": sku_name,
                        "page_price": price,
                        "sku_count": len(sku_info_map),
                        "price_changed": "否",
                        "name_changed": "否",
                        "count_changed": "否",
                    }
                    print(sku_row)

                    self.logger.debug(f"解析到SKU: {sku_row}")
                    sku_rows.append(sku_row)

        except Exception as e:
            self.logger.error(f"提取SKU失败: {e}")

        return sku_rows

    # -------------------------
    # 主入口
    # -------------------------
    async def run(self, url):

        html = await self.fetch_page(url)

        if not html:
            return []

        data_match = re.findall(
            r'window\.contextPath,(.*?)\);',
            html,
            re.S
        )

        if not data_match:
            print("未匹配到数据")
            return []

        data = data_match[0]

        json_data = self.extract_valid_json(data)

        if not json_data:
            return []

        sku_rows = self.extract_sku_rows(json_data,url)

        print(f"\n成功提取 {len(sku_rows)} 个SKU")

        return sku_rows




if __name__ == '__main__':
    # 创建 RequestManager，开启代理
    req_mgr = RequestManager(
    job="crawler",
    use_proxy=True,
    proxy_strategy="random",
    max_retries=3
)

    fetcher = AlibabaSkuFetcher(request_manager=req_mgr)

    url = "https://detail.1688.com/offer/968280048908.html"

    sku_list = asyncio.run(fetcher.run(url))

    # 完成后关闭 session
    req_mgr.close()