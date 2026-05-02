# utils/CookiePool.py
import json
import random
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from utils.logger import get_logger
from utils.dingtalk_bot import ding_bot_send


class CookiePool:
    def __init__(self, job: str, cookie_file: Path = None):
        self.logger = get_logger(job)
        self.job = job

        # 存储路径
        if cookie_file is None:
            cookie_file = Path(__file__).parent.parent / "data" / "cookies_pool.json"
        self.cookie_file = cookie_file
        self.cookie_file.parent.mkdir(parents=True, exist_ok=True)

        # 内存缓存
        self.cookies: List[Dict] = []
        self.current_index = 0

        # 加载cookie
        self.load()

    def load(self):
        """从文件加载cookie"""
        if self.cookie_file.exists():
            try:
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cookies = data.get('cookies', [])
                self.logger.info(f'加载了{len(self.cookies)}个cookie')
            except Exception as e:
                self.logger.error(f'加载cookie失败：{e}')
                self.cookies = []
        else:
            self.logger.info('cookie文件不存在，初始化空池')
            self.cookies = []

    def save(self):
        """保存cookie到文件"""
        try:
            data = {
                'cookies': self.cookies,
                'last_update': datetime.now().isoformat()
            }
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.debug(f"保存了 {len(self.cookies)} 个cookie")
        except Exception as e:
            self.logger.error(f"保存cookie失败: {e}")

    def add(self, cookies: Dict[str, str], account: str):
        """
        添加或更新cookie
        :param cookies: cookie字典
        :param account: 账号名称，如 "ali1688"
        """
        # 检查是否已存在
        for c in self.cookies:
            if c.get('account') == account:
                c['cookies'] = cookies
                c['update_time'] = datetime.now().isoformat()
                c['fail_count'] = 0  # 刷新后重置失败计数
                self.save()
                self.logger.info(f'更新cookie: {account}')
                return

        # 不存在则添加
        self.cookies.append({
            'account': account,
            'cookies': cookies,
            'add_time': datetime.now().isoformat(),
            'update_time': datetime.now().isoformat(),
            'fail_count': 0,
            'use_count': 0
        })
        self.save()
        self.logger.info(f"添加cookie: {account}")

    def remove(self, account: str):
        """移除指定账号的cookie"""
        self.cookies = [c for c in self.cookies if c.get('account') != account]
        self.save()
        self.logger.info(f'移除cookie: {account}')

    def get(self, strategy: str = "round_robin") -> Optional[Dict[str, str]]:
        """
        获取一个cookie，返回cookie字典
        """
        if not self.cookies:
            self.logger.warning("cookie池为空")
            return None

        # 过滤掉失败次数过多的
        valid_cookies = [c for c in self.cookies if c.get('fail_count', 0) < 3]

        if not valid_cookies:
            self.logger.warning("所有cookie都失效了")
            return None

        # 根据策略选择
        if strategy == "random":
            selected = random.choice(valid_cookies)
        elif strategy == "least_fail":
            selected = min(valid_cookies, key=lambda x: x.get('fail_count', 0))
        else:  # round_robin
            self.current_index = (self.current_index + 1) % len(valid_cookies)
            selected = valid_cookies[self.current_index]

        # 更新使用计数
        selected['use_count'] = selected.get('use_count', 0) + 1
        self.save()

        self.logger.debug(f"获取cookie: {selected.get('account')}")
        print(f'在使用 {selected.get("account")} 的cookie')

        # 返回cookie字典，并在内部记录当前账号
        cookie_dict = selected.get('cookies')
        if cookie_dict is not None:
            # 创建新字典，避免修改原数据
            result = dict(cookie_dict)
            result['__account'] = selected.get('account')
            return result

        return cookie_dict

    def mark_result(self, cookies: Dict[str, str], success: bool):
        """标记cookie使用结果"""
        # 从cookie字典中提取account信息
        account = cookies.pop('__account', None) if isinstance(cookies, dict) else None

        for c in self.cookies:
            if account and c.get('account') == account:
                self._update_cookie_result(c, success)
                break
            elif c['cookies'] == cookies:
                self._update_cookie_result(c, success)
                break

    def _update_cookie_result(self, cookie_record: Dict, success: bool):
        """更新单个cookie记录的结果"""
        account = cookie_record.get('account')
        if success:
            cookie_record['fail_count'] = 0
            self.logger.debug(f"cookie使用成功: {account}")
        else:
            cookie_record['fail_count'] = cookie_record.get('fail_count', 0) + 1
            self.logger.warning(f"cookie使用失败: {account}, 失败次数: {cookie_record['fail_count']}")

            # 连续失败3次，发送通知
            if cookie_record['fail_count'] >= 3:
                ding_bot_send('me', f"[{self.job}] Cookie失效: {account}")

        self.save()

    def get_by_account(self, account: str) -> Optional[Dict[str, str]]:
        """根据账号获取cookie"""
        for c in self.cookies:
            if c.get('account') == account:
                return c.get('cookies')
        return None

    def get_failed_accounts(self) -> List[str]:
        """获取所有失败次数达到上限的账号"""
        return [c.get('account') for c in self.cookies if c.get('fail_count', 0) >= 3]

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            'total': len(self.cookies),
            'valid': len([c for c in self.cookies if c.get('fail_count', 0) < 3]),
            'invalid': len([c for c in self.cookies if c.get('fail_count', 0) >= 3]),
            'total_uses': sum(c.get('use_count', 0) for c in self.cookies),
            'accounts': [c.get('account') for c in self.cookies]
        }