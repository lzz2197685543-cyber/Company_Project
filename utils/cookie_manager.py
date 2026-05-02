# utils/cookie_manager.py
import json
from pathlib import Path
from typing import Dict, Optional
from utils.CookiePool import CookiePool
from utils.logger import get_logger
from core.ali1688login import Ali1688Login


class CookieManager:
    """Cookie 管理器（支持单 Cookie 或池模式）"""

    def __init__(self, job: str, use_pool: bool = False):
        self.job = job
        self.use_pool = use_pool
        self.cookie_file = Path(__file__).parent.parent / "data" / "cookies" / f"ali1688_cookie.json"
        self.logger = get_logger(job)

        if use_pool:
            self.pool = CookiePool(job)
        else:
            self.pool = None

        # 记录当前使用的账号
        self.current_account = None


    async def get_auth(self) -> Optional[Dict[str, str]]:
        """获取认证信息，返回cookie字典"""
        if self.pool:
            cookies = self.pool.get()
            if cookies and '__account' in cookies:
                self.current_account = cookies['__account']
            return cookies
        else:
            # 单Cookie模式
            if self.cookie_file.exists():
                with open(self.cookie_file, 'r') as f:
                    cookies = json.load(f)
                    # 单Cookie模式，使用默认账号名
                    self.current_account = "ali1688"
                    return cookies
            return None

    def mark_result(self, cookies: Dict[str, str], success: bool):
        """标记 Cookie 使用结果"""
        if self.pool:
            self.pool.mark_result(cookies, success)

    async def refresh_by_account(self, account_name: str):
        """
        为指定账号刷新Cookie
        :param account_name: 账号名称，如 "ali1688" 或 "ali16880"
        """
        self.logger.info(f"开始刷新账号 {account_name} 的Cookie")
        # 验证账号名是否有效
        if not account_name:
            self.logger.error("账号名称为空，无法刷新")
            return False

        login = Ali1688Login(
            headless=False,
            use_user_data=True,
            job=self.job,
            account_name=account_name
        )

        try:
            login.init_browser()
            if login.ensure_login():
                browser_cookies = login.page.cookies()
                cookie_dict = {c['name']: c['value'] for c in browser_cookies}

                if self.pool:
                    # 更新指定账号的Cookie
                    self.pool.add(cookie_dict, account=account_name)
                    self.logger.info(f"账号 {account_name} 的Cookie已刷新并更新到池中")
                else:
                    # 保存到单Cookie文件
                    with open(self.cookie_file, 'w') as f:
                        json.dump(cookie_dict, f)
                    self.logger.info(f"账号 {account_name} 的单Cookie已刷新")
                return True
            else:
                self.logger.error(f"账号 {account_name} 登录失败")
                return False
        finally:
            login.close()

    async def refresh_failed_accounts(self):
        """刷新所有失效的账号"""
        if not self.pool:
            self.logger.warning("未使用Cookie池，无法刷新失效账号")
            return []

        failed_accounts = self.pool.get_failed_accounts()
        success_accounts = []

        for account in failed_accounts:
            self.logger.info(f"刷新失效账号: {account}")
            if await self.refresh_by_account(account):
                success_accounts.append(account)

        return success_accounts

    async def refresh(self):
        """兼容旧代码：刷新默认账号"""
        return await self.refresh_by_account("ali1688")

    def get_pool_stats(self) -> Dict:
        """获取池统计信息"""
        if self.pool:
            return self.pool.get_stats()
        return {}
