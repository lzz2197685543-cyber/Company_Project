import time
import random
import requests
from typing import Optional
from utils.headermanager import HeaderManager
from utils.proxy_pool import ProxyPool
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send
from urllib.parse import urlparse

class RequestManager:
    """请求管理器，整合请求头和代理池"""



    def __init__(self,
                 job,
                 header_manager: Optional[HeaderManager] = None,
                 use_proxy: bool = True,
                 max_retries: int = 6):
        """
        初始化请求管理器

        Args:
            header_manager: 请求头管理器
            use_proxy: 是否使用代理
            max_retries: 最大重试次数
        """
        self.header_manager = header_manager or HeaderManager()
        self.use_proxy = use_proxy
        self.max_retries = max_retries
        self.session = requests.Session()

        # 代理池
        self.proxy_pool = ProxyPool(job=job) if use_proxy else None
        self.current_proxy = None
        self.current_proxies = None

        self.logger=get_logger(job)

        self._proxy_acquired = False  # 添加标记，是否已获取代理

        # 各国家默认邮编
        self.AMAZON_ZIPCODE_MAP = {
            "amazon.com": "90002",  # 美国
            "amazon.co.uk": "SW1A1AA",  # 英国
            "amazon.de": "10115",  # 德国
            "amazon.fr": "75001",  # 法国
            "amazon.it": "00118",  # 意大利
            "amazon.es": "28001",  # 西班牙
            "amazon.ca": "M5V2T6",  # 加拿大
            "amazon.co.jp": "1000001",  # 日本
        }

        self.amazon_zipcode_set = set()

    def detect_amazon_domain(self,url):
        """根据URL识别Amazon国家站点"""
        try:
            domain = urlparse(url).netloc
            for d in self.AMAZON_ZIPCODE_MAP:
                if d in domain:
                    return d
        except:
            pass

        return None

    def set_amazon_zipcode(self, domain):
        """设置Amazon邮编"""
        zipcode = self.AMAZON_ZIPCODE_MAP.get(domain)

        if not zipcode:
            self.logger.info("未找到对应国家邮编")
            return

        print('domain:',domain)

        url = f"https://www.{domain}/portal-migration/hz/glow/address-change?actionSource=glow"

        payload = {
            "locationType": "LOCATION_INPUT",
            "zipCode": zipcode,
            "deviceType": "web",
            "storeContext": "generic",
            "pageType": "Gateway",
            "actionSource": "glow"
        }

        headers = self.header_manager.generate_amazon_headers()
        headers["content-type"] = "application/json"

        try:
            r = self.session.post(
                url,
                json=payload,
                headers=headers,
                timeout=10
            )

            if r.status_code == 200:
                self.logger.info(f"✅ 已设置邮编 {zipcode} ({domain})")
            else:
                self.logger.warning(f"⚠️ 邮编设置失败 {r.status_code}")

        except Exception as e:
            self.logger.error(f"设置邮编失败: {e}")

    def get(self, url: str, use_amazon_headers: bool = False, **kwargs):

        # 自动设置Amazon邮编
        domain = self.detect_amazon_domain(url)
        if domain and domain not in self.amazon_zipcode_set:
            self.set_amazon_zipcode(domain)
            self.amazon_zipcode_set.add(domain)

            # 🔴 设置邮编后，额外添加区域cookies
            self.session.cookies.set('i18n-prefs', 'USD', domain='.amazon.com')
            self.session.cookies.set('lc', 'en_US', domain='.amazon.com')
            self.session.cookies.set('session-id-time', '2082787201l', domain='.amazon.com')

        for attempt in range(1, self.max_retries + 1):
            try:
                # 生成请求头
                if use_amazon_headers:
                    headers = self.header_manager.generate_amazon_headers()
                else:
                    headers = self.header_manager.generate_headers()

                # 合并自定义请求头
                if 'headers' in kwargs:
                    headers.update(kwargs.pop('headers'))

                # 获取代理
                proxies = None
                proxy_info = "无代理"

                if self.use_proxy and self.proxy_pool:
                    # 如果还没有获取过代理，或者当前代理失效，才获取新代理
                    if not self._proxy_acquired or not self.current_proxy:
                        result = self.proxy_pool.get_proxy()
                        if result:
                            self.current_proxy, self.current_proxies = result
                            self._proxy_acquired = True
                            self.logger.info(f"📡 获取新代理: {self.current_proxies['http']}")
                    else:
                        self.logger.info(f"📡 复用当前代理: {self.current_proxies['http']}")

                    if self.current_proxy:
                        proxies = self.current_proxies
                        proxy_info = f"{self.current_proxy['host']}:{self.current_proxy['port']}"

                self.logger.info(f"使用代理: {proxy_info} (尝试 {attempt}/{self.max_retries})")

                # 随机延迟
                delay = random.uniform(0, 1)
                self.logger.info(f"⏳ 等待 {delay:.1f} 秒...")
                time.sleep(delay)

                # 发送请求
                self.logger.info(f"请求URL: {url}")

                response = self.session.get(
                    url,
                    headers=headers,
                    proxies=proxies,
                    timeout=kwargs.get('timeout', 30),
                    **{k: v for k, v in kwargs.items() if k not in ['headers', 'timeout']}
                )

                # 处理响应
                if response.status_code == 200:
                    self.logger.info(f"✅ 请求成功 - 状态码: {response.status_code}")
                    if self.current_proxy:
                        self.proxy_pool.report_success(self.current_proxy)

                    # 可选：验证出口IP
                    if self.current_proxy:
                        try:
                            # 测试当前代理的出口IP
                            test_response = requests.get(
                                'http://httpbin.org/ip',
                                proxies=proxies,
                                timeout=5
                            )
                            if test_response.status_code == 200:
                                print(f"📍 出口IP: {test_response.json().get('origin', 'unknown')}")
                        except:
                            pass

                    return response
                elif response.status_code in [403, 429]:
                    self.logger.error(f"⚠️ 请求被拒绝 ({response.status_code})，切换代理...")
                    if self.current_proxy:
                        self.proxy_pool.report_failure(self.current_proxy)
                    self.current_proxy = None
                    time.sleep(5)
                else:
                    self.logger.error(f"⚠️ 非预期状态码: {response.status_code}")

            except requests.exceptions.ProxyError as e:
                self.logger.error(f"❌ 代理错误: {type(e).__name__} - {str(e)}")
                if self.current_proxy:
                    self.proxy_pool.report_failure(self.current_proxy)
                self.current_proxy = None

            except requests.exceptions.ConnectionError as e:
                self.logger.error(f"❌ 连接错误: {type(e).__name__}")
                if self.current_proxy:
                    self.proxy_pool.report_failure(self.current_proxy)
                self.current_proxy = None

            except requests.exceptions.Timeout as e:
                self.logger.error(f"❌ 超时错误: {type(e).__name__}")
                if self.current_proxy:
                    self.proxy_pool.report_failure(self.current_proxy)
                self.current_proxy = None

            except Exception as e:
                self.logger.error(f"❌ 请求失败: {type(e).__name__} - {str(e)}")
                if self.current_proxy:
                    self.proxy_pool.report_failure(self.current_proxy)
                self.current_proxy = None

            # 重试前等待
            if attempt < self.max_retries:
                wait_time = random.uniform(2, 5)
                self.logger.info(f"⏰ 等待 {wait_time:.1f} 秒后重试...")
                time.sleep(wait_time)

        self.logger.info(f"❌ 所有重试失败: {url}")
        ding_bot_send('me',f"[获取亚马逊优惠券]--所有重试失败: {url}")
        return None

    def close(self):
        """关闭session"""
        self.session.close()