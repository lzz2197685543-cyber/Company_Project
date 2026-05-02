import time
import random
import requests
from typing import Dict, Optional, Tuple
from utils.headermanager import HeaderManager
from utils.proxy_pool import ProxyPool
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send

class RequestManager:
    """通用请求管理器，整合请求头和代理池，支持多种代理策略"""

    def __init__(self,
                 job: str,
                 header_manager: Optional[HeaderManager] = None,
                 use_proxy: bool = True,
                 max_retries: int = 6,
                 proxy_strategy: str = "random",  # "reuse", "random", "adaptive"
                 max_requests_per_proxy: int = 20,  # 每个代理最大请求次数（0不限制）
                 slow_response_threshold: float = 5.0,  # 慢响应阈值（秒）
                 max_slow_responses: int = 3,  # 最大慢响应次数
                 enable_stats: bool = True):  # 是否启用统计
        """
        初始化请求管理器

        Args:
            job: 任务名称
            header_manager: 请求头管理器
            use_proxy: 是否使用代理
            max_retries: 最大重试次数
            proxy_strategy: 代理策略 ("reuse", "random", "adaptive")
            max_requests_per_proxy: 每个代理最多使用的请求次数（0表示不限制）
            slow_response_threshold: 慢响应阈值（秒）
            max_slow_responses: 最大慢响应次数，超过后切换代理
            enable_stats: 是否启用代理统计
        """
        self.job = job
        self.header_manager = header_manager or HeaderManager()
        self.use_proxy = use_proxy
        self.max_retries = max_retries
        self.proxy_strategy = proxy_strategy
        self.max_requests_per_proxy = max_requests_per_proxy
        self.slow_response_threshold = slow_response_threshold
        self.max_slow_responses = max_slow_responses
        self.enable_stats = enable_stats

        # Session管理
        self.session = requests.Session()

        # 代理池
        self.proxy_pool = ProxyPool(job=job) if use_proxy else None
        self.current_proxy: Optional[Dict] = None
        self.current_proxies: Optional[Dict] = None
        self._proxy_acquired = False
        self.requests_this_proxy = 0

        # 质量监控
        self._slow_response_count = 0
        self._consecutive_failures = 0

        # 代理统计
        self._proxy_stats: Dict[str, Dict] = {} if enable_stats else None

        # 日志
        self.logger = get_logger(job)

        self.logger.info(f"🚀 请求管理器初始化完成 - 策略: {proxy_strategy}, "
                         f"代理: {'启用' if use_proxy else '禁用'}, "
                         f"最大重试: {max_retries}, "
                         f"代理上限: {max_requests_per_proxy if max_requests_per_proxy > 0 else '不限'}")





    def _get_proxy(self) -> Tuple[Optional[Dict], Optional[Dict]]:
        """
        根据策略获取代理

        Returns:
            (proxy_info, proxies_dict) 元组
        """
        if not self.use_proxy or not self.proxy_pool:
            return None, None

        # 自适应策略：动态判断是否需要切换
        if self.proxy_strategy == "adaptive":
            need_switch = False

            # 检查使用次数限制
            if (self.current_proxy and
                    self.max_requests_per_proxy > 0 and
                    self.requests_this_proxy >= self.max_requests_per_proxy):
                self.logger.info(f"🔄 代理已达使用上限({self.max_requests_per_proxy}次)，切换代理")
                need_switch = True

            # 检查连续失败
            if self._consecutive_failures >= 2:
                self.logger.info(f"🔄 连续失败{self._consecutive_failures}次，切换代理")
                need_switch = True

            # 需要切换时获取新代理
            if need_switch:
                self._switch_proxy()

            # 复用当前代理
            if self._proxy_acquired and self.current_proxy:
                self.logger.debug(f"📡 复用当前代理，已使用 {self.requests_this_proxy} 次")
                return self.current_proxy, self.current_proxies

            # 如果没有代理，获取新代理
            if not self._proxy_acquired or not self.current_proxy:
                self._switch_proxy()

        # 随机策略：每次都获取新代理
        elif self.proxy_strategy == "random":
            self.logger.debug("🎲 随机策略：获取新代理")
            self._switch_proxy()

        # 复用策略：只在没有代理时获取
        elif self.proxy_strategy == "reuse":
            if not self._proxy_acquired or not self.current_proxy:
                self._switch_proxy()
            else:
                self.logger.debug(f"📡 复用当前代理，已使用 {self.requests_this_proxy} 次")
                return self.current_proxy, self.current_proxies

        return self.current_proxy, self.current_proxies

    def _switch_proxy(self) -> None:
        """切换代理"""
        if not self.proxy_pool:
            return

        result = self.proxy_pool.get_proxy()
        if result:
            self.current_proxy, self.current_proxies = result
            self._proxy_acquired = True
            self.requests_this_proxy = 0
            self._consecutive_failures = 0
            proxy_url = self.current_proxies.get('http', 'unknown')
            self.logger.info(f"📡 获取新代理: {proxy_url}")
        else:
            self.logger.warning("⚠️ 代理池无可用代理")
            self.current_proxy = None
            self.current_proxies = None
            self._proxy_acquired = False

    def _check_proxy_quality(self, response_time: float) -> None:
        """
        检查代理质量（自适应策略用）

        Args:
            response_time: 响应时间（秒）
        """
        if response_time > self.slow_response_threshold:
            self.logger.warning(f"⚠️ 代理响应慢 ({response_time:.2f}秒)，阈值: {self.slow_response_threshold}秒")
            self._slow_response_count += 1

            if self._slow_response_count >= self.max_slow_responses:
                self.logger.warning(f"🔄 连续{self._slow_response_count}次慢响应，切换代理")
                self._switch_proxy()
                self._slow_response_count = 0
        else:
            # 响应正常，重置慢响应计数
            if self._slow_response_count > 0:
                self._slow_response_count = 0

    def _update_proxy_stats(self, proxy: Optional[Dict], success: bool, response_time: float = 0) -> None:
        """
        更新代理统计信息

        Args:
            proxy: 代理信息
            success: 是否成功
            response_time: 响应时间
        """
        if not self.enable_stats or not proxy:
            return

        proxy_key = f"{proxy.get('host', 'unknown')}:{proxy.get('port', 'unknown')}"

        if proxy_key not in self._proxy_stats:
            self._proxy_stats[proxy_key] = {
                'total': 0,
                'success': 0,
                'fail': 0,
                'total_time': 0,
                'avg_time': 0,
                'last_used': 0
            }

        stats = self._proxy_stats[proxy_key]
        stats['total'] += 1
        stats['last_used'] = time.time()

        if success:
            stats['success'] += 1
            stats['total_time'] += response_time
            stats['avg_time'] = stats['total_time'] / stats['success']
        else:
            stats['fail'] += 1

        # 记录到日志（调试级别）
        if stats['total'] % 20 == 0:  # 每20次记录一次
            success_rate = (stats['success'] / stats['total']) * 100
            self.logger.debug(f"📊 代理 {proxy_key} 统计: 成功率={success_rate:.1f}%, "
                              f"平均响应={stats['avg_time']:.2f}秒")

    def _verify_outgoing_ip(self, proxies: Optional[Dict]) -> None:
        """
        验证出口IP（调试用）

        Args:
            proxies: 代理配置
        """
        try:
            test_response = requests.get(
                'http://httpbin.org/ip',
                proxies=proxies,
                timeout=5
            )
            if test_response.status_code == 200:
                ip_info = test_response.json().get('origin', 'unknown')
                self.logger.debug(f"📍 出口IP: {ip_info}")
        except Exception as e:
            self.logger.debug(f"无法获取出口IP: {e}")

    def _should_retry(self, status_code: int) -> bool:
        """
        判断是否应该重试

        Args:
            status_code: HTTP状态码

        Returns:
            是否应该重试
        """
        # 客户端错误（4xx）通常不应该重试，除了429（请求过多）
        if 400 <= status_code < 500:
            return status_code == 429
        # 服务器错误（5xx）应该重试
        return status_code >= 500

    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        发送GET请求

        Args:
            url: 请求URL
            **kwargs: 其他requests参数

        Returns:
            响应对象，失败返回None
        """
        for attempt in range(1, self.max_retries + 1):
            proxy_info = "无代理"
            proxies = None
            proxy = None



            try:
                # 生成通用请求头
                headers = self.header_manager.generate_headers()

                # 合并自定义请求头
                if 'headers' in kwargs:
                    headers.update(kwargs.pop('headers'))

                # 获取代理
                proxy, proxies = self._get_proxy()

                if proxy and proxies:
                    proxy_info = f"{proxy.get('host', 'unknown')}:{proxy.get('port', 'unknown')}"

                self.logger.info(f"📡 使用代理: {proxy_info} (尝试 {attempt}/{self.max_retries})")

                # 随机延迟，避免请求过快
                delay = random.uniform(0.5, 1.5)
                if attempt > 1:  # 重试时增加延迟
                    delay = random.uniform(2, 5)
                self.logger.debug(f"⏳ 等待 {delay:.1f} 秒...")
                time.sleep(delay)

                # 记录开始时间
                start_time = time.time()

                # 发送请求
                self.logger.info(f"🔗 GET请求: {url[:100]}{'...' if len(url) > 100 else ''}")

                response = self.session.get(
                    url,
                    headers=headers,
                    proxies=proxies,
                    timeout=kwargs.get('timeout', 30),
                    allow_redirects=kwargs.get('allow_redirects', True),
                    **{k: v for k, v in kwargs.items() if k not in ['headers', 'timeout', 'allow_redirects']}
                )

                # 计算响应时间
                response_time = time.time() - start_time

                # 处理响应
                if response.status_code == 200:
                    self.logger.info(f"✅ 请求成功 - 状态码: {response.status_code}, "
                                     f"耗时: {response_time:.2f}秒, "
                                     f"大小: {len(response.content)} bytes")

                    if proxy:
                        self.proxy_pool.report_success(proxy)
                        self.requests_this_proxy += 1
                        self._update_proxy_stats(proxy, True, response_time)
                        self._consecutive_failures = 0  # 重置连续失败计数

                    # 自适应策略：检查代理质量
                    if self.proxy_strategy == "adaptive" and proxy:
                        self._check_proxy_quality(response_time)

                    # 可选：验证出口IP（调试模式）
                    if self.logger.level <= 10 and proxy:  # DEBUG级别
                        self._verify_outgoing_ip(proxies)

                    return response

                elif self._should_retry(response.status_code):
                    self.logger.warning(f"⚠️ 请求失败 ({response.status_code})，将重试...")
                    if proxy:
                        self.proxy_pool.report_failure(proxy)
                        self._update_proxy_stats(proxy, False)
                        self._consecutive_failures += 1

                    # 对于429和503，等待更长时间
                    if response.status_code in [429, 503]:
                        wait_time = random.uniform(5, 10)
                        self.logger.info(f"⏰ 遇到{response.status_code}，等待 {wait_time:.1f} 秒...")
                        time.sleep(wait_time)

                    # 根据策略决定是否切换代理
                    if self.proxy_strategy in ["random", "adaptive"]:
                        self._switch_proxy()
                else:
                    # 非重试状态码，直接返回None
                    self.logger.error(f"❌ 非预期状态码: {response.status_code}，停止重试")
                    if proxy:
                        self.proxy_pool.report_failure(proxy)
                        self._update_proxy_stats(proxy, False)
                    return None

            except requests.exceptions.ProxyError as e:
                self.logger.error(f"❌ 代理错误: {type(e).__name__}")
                if proxy:
                    self.proxy_pool.report_failure(proxy)
                    self._update_proxy_stats(proxy, False)
                    self._consecutive_failures += 1
                self._switch_proxy()

            except requests.exceptions.ConnectionError as e:
                self.logger.error(f"❌ 连接错误: {type(e).__name__}")
                if proxy:
                    self.proxy_pool.report_failure(proxy)
                    self._update_proxy_stats(proxy, False)
                    self._consecutive_failures += 1
                self._switch_proxy()

            except requests.exceptions.Timeout as e:
                self.logger.error(f"❌ 超时错误: {type(e).__name__}")
                if proxy:
                    self.proxy_pool.report_failure(proxy)
                    self._update_proxy_stats(proxy, False)
                    self._consecutive_failures += 1
                self._switch_proxy()

            except Exception as e:
                self.logger.error(f"❌ 请求失败: {type(e).__name__} - {str(e)}")
                if proxy:
                    self.proxy_pool.report_failure(proxy)
                    self._update_proxy_stats(proxy, False)
                    self._consecutive_failures += 1
                self._switch_proxy()

            # 重试前等待
            if attempt < self.max_retries:
                wait_time = random.uniform(2, 5)
                self.logger.info(f"⏰ 等待 {wait_time:.1f} 秒后重试...")
                time.sleep(wait_time)

        self.logger.error(f"❌ 所有重试失败: {url}")
        ding_bot_send('me', f"[{self.job}] 请求失败: {url}")
        return None

    def post(self, url: str, data: Optional[Dict] = None,
             json: Optional[Dict] = None, **kwargs) -> Optional[requests.Response]:
        """
        发送POST请求

        Args:
            url: 请求URL
            data: 表单数据
            json: JSON数据
            **kwargs: 其他requests参数

        Returns:
            响应对象，失败返回None
        """
        for attempt in range(1, self.max_retries + 1):
            proxy_info = "无代理"
            proxies = None
            proxy = None

            try:
                # 生成通用请求头
                headers = self.header_manager.generate_headers()

                # 如果是JSON请求，添加Content-Type
                if json is not None and 'Content-Type' not in headers:
                    headers['Content-Type'] = 'application/json'

                # 合并自定义请求头
                if 'headers' in kwargs:
                    headers.update(kwargs.pop('headers'))

                # 获取代理
                proxy, proxies = self._get_proxy()

                if proxy and proxies:
                    proxy_info = f"{proxy.get('host', 'unknown')}:{proxy.get('port', 'unknown')}"

                self.logger.info(f"📡 使用代理: {proxy_info} (尝试 {attempt}/{self.max_retries})")

                # 随机延迟
                delay = random.uniform(0.5, 1.5)
                if attempt > 1:
                    delay = random.uniform(2, 5)
                time.sleep(delay)

                # 记录开始时间
                start_time = time.time()

                self.logger.info(f"🔗 POST请求: {url[:100]}{'...' if len(url) > 100 else ''}")

                response = self.session.post(
                    url,
                    headers=headers,
                    data=data,
                    json=json,
                    proxies=proxies,
                    timeout=kwargs.get('timeout', 30),
                    **{k: v for k, v in kwargs.items() if k not in ['headers', 'timeout', 'data', 'json']}
                )

                # 计算响应时间
                response_time = time.time() - start_time

                if response.status_code == 200:
                    self.logger.info(f"✅ POST请求成功 - 状态码: {response.status_code}, "
                                     f"耗时: {response_time:.2f}秒")

                    if proxy:
                        self.proxy_pool.report_success(proxy)
                        self.requests_this_proxy += 1
                        self._update_proxy_stats(proxy, True, response_time)
                        self._consecutive_failures = 0

                    # 自适应策略：检查代理质量
                    if self.proxy_strategy == "adaptive" and proxy:
                        self._check_proxy_quality(response_time)

                    return response

                elif self._should_retry(response.status_code):
                    self.logger.warning(f"⚠️ POST请求失败 ({response.status_code})，将重试...")
                    if proxy:
                        self.proxy_pool.report_failure(proxy)
                        self._update_proxy_stats(proxy, False)
                        self._consecutive_failures += 1

                    if response.status_code in [429, 503]:
                        wait_time = random.uniform(5, 10)
                        self.logger.info(f"⏰ 遇到{response.status_code}，等待 {wait_time:.1f} 秒...")
                        time.sleep(wait_time)

                    if self.proxy_strategy in ["random", "adaptive"]:
                        self._switch_proxy()
                else:
                    self.logger.error(f"❌ 非预期状态码: {response.status_code}，停止重试")
                    if proxy:
                        self.proxy_pool.report_failure(proxy)
                        self._update_proxy_stats(proxy, False)
                    return None

            except requests.exceptions.ProxyError as e:
                self.logger.error(f"❌ 代理错误: {type(e).__name__}")
                if proxy:
                    self.proxy_pool.report_failure(proxy)
                    self._update_proxy_stats(proxy, False)
                    self._consecutive_failures += 1
                self._switch_proxy()

            except Exception as e:
                self.logger.error(f"❌ POST请求失败: {type(e).__name__} - {str(e)}")
                if proxy:
                    self.proxy_pool.report_failure(proxy)
                    self._update_proxy_stats(proxy, False)
                    self._consecutive_failures += 1
                self._switch_proxy()

            if attempt < self.max_retries:
                wait_time = random.uniform(2, 5)
                self.logger.info(f"⏰ 等待 {wait_time:.1f} 秒后重试...")
                time.sleep(wait_time)

        self.logger.error(f"❌ POST请求所有重试失败: {url}")
        return None

    def get_proxy_stats(self) -> Dict:
        """
        获取代理统计信息

        Returns:
            代理统计字典
        """
        if not self.enable_stats:
            self.logger.warning("统计功能未启用")
            return {}
        return self._proxy_stats

    def get_current_proxy(self) -> Optional[Dict]:
        """获取当前使用的代理"""
        return self.current_proxy

    def reset_proxy_stats(self) -> None:
        """重置代理统计信息"""
        if self.enable_stats:
            self._proxy_stats.clear()
            self.logger.info("📊 代理统计信息已重置")

    def close(self) -> None:
        """关闭session"""
        self.session.close()
        self.logger.info("🔌 请求管理器已关闭")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()


# 使用示例
def example_usage():
    """使用示例"""

    # 示例1: 复用策略（适合需要保持会话的场景）
    with RequestManager(
            job="example_task",
            use_proxy=True,
            proxy_strategy="reuse",
            max_retries=3
    ) as rm:
        response = rm.get("https://httpbin.org/ip")
        if response:
            print(response.json())

    # 示例2: 随机策略（适合大规模爬取）
    rm_random = RequestManager(
        job="crawler",
        use_proxy=True,
        proxy_strategy="random",
        max_retries=3
    )

    # 发送多个请求，每次可能使用不同代理
    for i in range(5):
        response = rm_random.get("https://httpbin.org/ip")
        if response:
            print(f"请求{i + 1}: {response.json()}")
        time.sleep(1)

    rm_random.close()

    # 示例3: 自适应策略（平衡性能）
    rm_adaptive = RequestManager(
        job="smart_crawler",
        use_proxy=True,
        proxy_strategy="adaptive",
        max_requests_per_proxy=15,  # 每个代理最多15次请求
        slow_response_threshold=3.0,  # 3秒算慢响应
        max_slow_responses=2,  # 连续2次慢响应切换
        enable_stats=True
    )

    # 发送请求
    urls = [
        "https://httpbin.org/ip",
        "https://httpbin.org/get",
        "https://httpbin.org/headers"
    ]

    for url in urls:
        response = rm_adaptive.get(url)
        if response:
            print(f"✅ {url}: {response.status_code}")
        time.sleep(1)

    # 查看统计
    stats = rm_adaptive.get_proxy_stats()
    print("\n代理统计:")
    for proxy_id, stat in stats.items():
        success_rate = (stat['success'] / stat['total']) * 100 if stat['total'] > 0 else 0
        print(f"  {proxy_id}: 成功率={success_rate:.1f}%, "
              f"平均响应={stat['avg_time']:.2f}秒")

    rm_adaptive.close()


if __name__ == '__main__':
    example_usage()