# scripts/init_cookie_pool.py
import asyncio
from core.ali1688login import Ali1688Login
from utils.CookiePool import CookiePool
from utils.logger import get_logger


async def init_cookie_pool():
    """初始化cookie池，为配置文件中的所有1688账号获取cookie"""
    logger = get_logger("init_cookie_pool")
    pool = CookiePool("init_cookie_pool")

    # 配置文件中所有1688账号
    accounts = ["ali1688", "ali16880","ali16881"]

    for account_name in accounts:
        logger.info(f"正在为账号 {account_name} 获取cookie...")

        login = Ali1688Login(
            headless=False,
            use_user_data=True,
            account_name=account_name
        )

        try:
            login.init_browser()
            if login.ensure_login():
                browser_cookies = login.page.cookies()
                cookie_dict = {c['name']: c['value'] for c in browser_cookies}

                # 添加到池中
                pool.add(cookie_dict, account=account_name)
                logger.info(f"账号 {account_name} 的cookie已添加到池中")
            else:
                logger.error(f"账号 {account_name} 登录失败")
        except Exception as e:
            logger.error(f"处理账号 {account_name} 时出错: {e}")
        finally:
            login.close()

    # 打印池状态
    stats = pool.get_stats()
    logger.info(f"Cookie池初始化完成: {stats}")


if __name__ == '__main__':
    asyncio.run(init_cookie_pool())