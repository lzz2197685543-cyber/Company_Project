import json
import random
import time
import threading
from typing import Dict, Optional, Tuple, List
import requests
from pathlib import Path
from utils.logger import get_logger

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"



class ProxyPool:
    """代理池管理器"""
    def __init__(self, job,proxy_file: str = CONFIG_DIR / 'proxy_pool.json'):
        self.logger = get_logger(job)
        self.proxies: List[Dict] = []
        self.proxy_stats: Dict[str, Dict] = {}
        self.lock = threading.Lock()
        self.load_proxies(proxy_file)

    def load_proxies(self, proxy_file: str):
        """加载代理配置"""
        try:
            with open(proxy_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.proxies = data.get('proxies', [])

                # 初始化统计信息
                for proxy in self.proxies:
                    proxy_id = self._get_proxy_id(proxy)
                    self.proxy_stats[proxy_id] = {
                        'fail_count': 0,
                        'is_active': True,
                        'success_count': 0
                    }
                self.logger.info(f"✅ 加载 {len(self.proxies)} 个代理")
        except Exception as e:
            self.logger.error(f"❌ 加载代理失败: {e}")
            self.proxies = []

    def _get_proxy_id(self, proxy: Dict) -> str:
        """获取代理ID"""
        return f"{proxy['host']}:{proxy['port']}"

    def _format_proxies(self, proxy: Dict) -> Dict[str, str]:
        """格式化代理为requests可用格式 - 修复SOCKS5格式"""
        # SOCKS5 的正确格式
        if proxy['type'] == 'socks5':
            proxy_url = f"socks5://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
        else:
            proxy_url = f"{proxy['type']}://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"

        return {
            'http': proxy_url,
            'https': proxy_url
        }

    def get_proxy(self) -> Optional[Tuple[Dict, Dict[str, str]]]:
        """获取随机可用代理"""
        with self.lock:
            # 筛选可用代理
            available = []
            for proxy in self.proxies:
                proxy_id = self._get_proxy_id(proxy)
                if self.proxy_stats[proxy_id]['is_active']:
                    available.append(proxy)

            # 如果没有可用代理，重置所有
            if not available:
                print("⚠️ 无可用代理，重置所有代理状态")
                for proxy_id in self.proxy_stats:
                    self.proxy_stats[proxy_id]['is_active'] = True
                    self.proxy_stats[proxy_id]['fail_count'] = 0
                available = self.proxies

            if not available:
                return None

            # 随机选择
            proxy = random.choice(available)
            return proxy, self._format_proxies(proxy)

    def report_success(self, proxy: Dict):
        """报告成功"""
        with self.lock:
            proxy_id = self._get_proxy_id(proxy)
            if proxy_id in self.proxy_stats:
                self.proxy_stats[proxy_id]['fail_count'] = 0
                self.proxy_stats[proxy_id]['success_count'] += 1

    def report_failure(self, proxy: Dict):
        """报告失败"""
        with self.lock:
            proxy_id = self._get_proxy_id(proxy)
            if proxy_id in self.proxy_stats:
                self.proxy_stats[proxy_id]['fail_count'] += 1
                # 连续失败3次标记为失效
                if self.proxy_stats[proxy_id]['fail_count'] >= 3:
                    self.proxy_stats[proxy_id]['is_active'] = False
                    print(f"⚠️ 代理 {proxy_id} 已标记为失效")

    def test_proxy(self, proxy: Dict) -> bool:
        """测试单个代理"""
        try:
            proxies = self._format_proxies(proxy)
            self.logger.info(f"测试代理: {proxy['host']}:{proxy['port']}")

            start = time.time()
            # 使用一个稳定的测试URL
            r = requests.get(
                'http://httpbin.org/ip',
                proxies=proxies,
                timeout=10,  # 增加超时时间
                headers={'User-Agent': 'Mozilla/5.0'}
            )

            if r.status_code == 200:
                response_time = time.time() - start
                self.logger.info(f"✅ {proxy['host']}:{proxy['port']} 可用 ({response_time:.2f}s)")
                self.logger.info(f"   出口IP: {r.json().get('origin', 'unknown')}")
                return True
            else:
                self.logger.info(f"❌ {proxy['host']}:{proxy['port']} 返回状态码: {r.status_code}")
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"❌ {proxy['host']}:{proxy['port']} 连接错误: {type(e).__name__}")
        except requests.exceptions.ProxyError as e:
            self.logger.error(f"❌ {proxy['host']}:{proxy['port']} 代理错误: {type(e).__name__}")
        except requests.exceptions.Timeout as e:
            self.logger.error(f"❌ {proxy['host']}:{proxy['port']} 超时")
        except Exception as e:
            self.logger.error(f"❌ {proxy['host']}:{proxy['port']} 错误: {type(e).__name__}")

        return False

    def test_all(self):
        """测试所有代理"""
        self.logger.info("\n开始测试所有代理...")
        self.logger.info("=" * 50)

        active_count = 0
        for proxy in self.proxies:
            is_available = self.test_proxy(proxy)
            proxy_id = self._get_proxy_id(proxy)

            with self.lock:
                self.proxy_stats[proxy_id]['is_active'] = is_available
                if is_available:
                    active_count += 1
                    self.proxy_stats[proxy_id]['fail_count'] = 0
                else:
                    self.proxy_stats[proxy_id]['fail_count'] += 1

            time.sleep(1)  # 避免请求过快

        self.logger.info("=" * 50)
        self.logger.info(f"📊 测试完成: {active_count}/{len(self.proxies)} 个代理可用")
        return active_count



def test_single_proxy():
    """测试单个代理是否可用"""
    proxy_pool = ProxyPool('11')

    print("=" * 60)
    print("代理测试工具")
    print("=" * 60)

    # 测试所有代理
    active = proxy_pool.test_all()

    if active == 0:
        print("\n❌ 没有可用的代理！")
        print("可能的原因：")
        print("1. 代理已过期或失效")
        print("2. 需要安装 requests[socks]: pip install requests[socks]")
        print("3. 代理服务器需要认证")
        return

    # 测试获取代理
    print("\n🔍 测试获取随机代理...")
    result = proxy_pool.get_proxy()
    if result:
        proxy, proxies = result
        print(f"✅ 获取到代理: {proxy['host']}:{proxy['port']}")
        print(f"   代理URL: {proxies['http']}")

        # 测试实际使用
        try:
            import requests
            r = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=10)
            print(f"✅ 代理工作正常，出口IP: {r.json().get('origin')}")
        except Exception as e:
            print(f"❌ 代理测试失败: {e}")
    else:
        print("❌ 无法获取代理")



if __name__ == '__main__':
    test_single_proxy()