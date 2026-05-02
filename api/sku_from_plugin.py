# core/ali1688login.py
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.common import Actions
from utils.dingtalk_bot import ding_bot_send
from utils.logger import get_logger
from bs4 import BeautifulSoup
from pathlib import Path
import random
import time
import re

Chrom_DIR = Path(__file__).resolve().parent.parent / "data"


class SkuFromPlugin:
    def __init__(self, headless=False, job=None):
        """
        初始化登录器
        :param headless: 是否无头模式
        :param job: 任务名称
        """

        self.headless = headless
        self.page = None
        self.actions = None
        self.logger = get_logger(job)

    def init_browser(self):
        try:
            # 创建一个配置对象，并启用自动端口功能
            co = ChromiumOptions()

            if self.headless:
                co.headless()

            # 关键：添加更多反检测参数
            co.set_argument('--disable-blink-features=AutomationControlled')
            co.set_argument('--disable-features=IsolateOrigins,site-per-process')
            co.set_argument('--disable-web-security')
            co.set_argument('--disable-infobars')
            co.set_argument('--disable-blink-features')
            co.set_argument('--no-sandbox')
            co.set_argument('--disable-dev-shm-usage')

            co.set_user_data_path(f"{Chrom_DIR}/my_chrome_data")  # 相对路径，就在你脚本所在的文件夹里

            # 设置更真实的 User-Agent
            co.set_user_agent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )

            # 设置窗口大小（真实用户常用）
            co.set_argument('--window-size=1920,1080')

            # 添加实验性选项，隐藏 webdriver 特征
            co.set_pref('credentials_enable_service', False)
            co.set_pref('profile.password_manager_enabled', False)

            # 初始化页面
            self.page = ChromiumPage(addr_or_opts=co)
            self.actions = Actions(self.page)

            # 注入 JavaScript 隐藏 webdriver 属性
            self.page.run_js("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            return True
        except Exception as e:
            print(f"浏览器初始化失败: {e}")
            raise

    def close(self):
        """关闭浏览器"""
        if self.page:
            self.page.quit()
            print(f"浏览器已关闭")

    def check_and_login(self):
        """检查登录状态，如未登录则执行登录"""
        try:
            self.page.get('https://www.dianleida.net/')
            time.sleep(3.5)


            if self.page.ele('xpath://*[@id="__layout"]/div/div[2]/div[2]/div[3]/div[1]/div[2]', timeout=3):
                self.login()
            elif self.page.ele('xpath:/html/body/div[3]/div/div[3]/span/button[2]/span', timeout=3):
                self.login()
            else:
                print('已经登录了，不需要再登录')

        except Exception as e:
            print(f"登录检查失败: {e}")
            raise

    def login(self):
        """执行登录操作"""
        try:
            self.page.get('https://www.dianleida.net/')
            time.sleep(2)

            # 点击密码登录
            password_login_btn = self.page.ele('xpath://*[@id="__layout"]/div/div[2]/div[2]/div[3]/div[1]/div[2]',
                                               timeout=5)
            if password_login_btn:
                password_login_btn.click()
                time.sleep(1)

            # 输入账号
            account_input = self.page.ele('xpath://*[@id="__layout"]/div/div[2]/div[2]/div[3]/div[2]/p/div/input',
                                          timeout=5)
            account_input.clear()
            account_input.input('18929089237')

            # 输入密码
            password_input = self.page.ele(
                'xpath://*[@id="__layout"]/div/div[2]/div[2]/div[3]/div[2]/div[1]/div[1]/input', timeout=5)
            password_input.clear()
            password_input.input('lxz2580hh')

            # 点击登录
            login_btn = self.page.ele('xpath://*[@id="__layout"]/div/div[2]/div[2]/div[3]/div[2]/div[3]/div[1]/button',
                                      timeout=5)
            login_btn.click()

            print("登录操作已提交，等待跳转...")
            time.sleep(3)

        except Exception as e:
            print(f"登录失败: {e}")
            raise

    def check_no_sku_popup(self):
        """
        增强版：检查是否有"暂无SKU"的提示弹窗
        """
        try:
            # 等待弹窗出现
            time.sleep(1)

            # 方法1：直接在页面HTML中搜索关键词
            page_html = self.page.html
            no_sku_keywords = ["暂无SKU", "该产品暂无SKU", "请刷新重试"]
            for keyword in no_sku_keywords:
                if keyword in page_html:
                    print(f"✅ 在页面源码中检测到关键词: {keyword}")
                    self._close_no_sku_popup()
                    return True

            # 方法2：查找包含这些文本的元素
            for keyword in no_sku_keywords:
                try:
                    element = self.page.ele(f'xpath://*[contains(text(), "{keyword}")]', timeout=2)
                    if element and element.is_displayed():
                        print(f"✅ 检测到弹窗文本: {element.text}")
                        self._close_no_sku_popup()
                        return True
                except:
                    continue

            return False

        except Exception as e:
            print(f"检查暂无SKU弹窗时出错: {e}")
            return False

    def _close_no_sku_popup(self):
        """
        关闭暂无SKU的提示弹窗
        """
        try:
            # 等待一下让弹窗完全显示
            time.sleep(0.5)

            # 尝试点击关闭按钮
            close_selectors = [
                'xpath://button[contains(@class, "dld-dialog__close")]',
                'xpath://i[contains(@class, "dld-icon-close")]',
                'xpath://*[contains(@class, "dld-dialog__headerbtn")]',
                'xpath://div[contains(@class, "dld-dialog")]//button[@aria-label="Close"]',
                'xpath://*[contains(text(), "确定")]',
                'xpath://*[contains(text(), "知道了")]'
            ]

            for selector in close_selectors:
                try:
                    close_btn = self.page.ele(selector, timeout=2)
                    if close_btn and close_btn.is_displayed():
                        close_btn.click()
                        print("✅ 已关闭提示弹窗")
                        time.sleep(0.5)
                        return
                except:
                    continue

            # 如果找不到关闭按钮，尝试按ESC键
            try:
                self.page.actions.type('Escape')
                print("✅ 已按ESC键关闭弹窗")
                time.sleep(0.5)
            except:
                pass

        except Exception as e:
            print(f"关闭弹窗时出错: {e}")

    def goto_goods(self, url):
        """跳转到商品详情页面并获取SKU数据"""
        try:
            # 访问目标商品页面
            print(f"正在访问商品页面: {url}")
            self.page.get(url)
            time.sleep(3)


            # 处理可能的验证页面
            if self.page.ele('xpath://*[@id="baxia-punish"]/div[2]/div/div[1]/div[2]', timeout=2):
                print("检测到验证页面，尝试关闭...")
                self.page.ele('xpath:/html/body/div[10]/img').click()
                time.sleep(1)

            # 等待并点击SKU分析按钮
            target_element = 'xpath://*[@id="content"]/div[2]/div[1]/div[2]/div[1]/div[2]/div[1]/div[2]'
            self.page.wait.ele_displayed(target_element, timeout=6)
            self.page.ele(target_element).click()
            print("已点击SKU分析按钮")


            # 【关键修改】点击后立即检查是否有"暂无SKU"弹窗
            # 给弹窗一点出现的时间
            time.sleep(1)


            # 检查弹窗
            if self.check_no_sku_popup():
                print("⚠️ 该产品暂无SKU数据")
                return []

            # 检测验证码并且滑动过验证码
            if self.page.ele('xpath://*[@id="nc_1__scale_text"]/span'):
                self.logger.info('14：检测到滑块')
                slider = self.page.ele('xpath://*[@id="nc_1_n1z"]')

                # # 获取实际需要拖动的距离
                # bg = self.page.ele('xpath://*[@id="nc_1_bg"]')
                # if bg:
                #     bg_width = bg.rect.width
                #     slider_width = slider.rect.width
                #     target_distance = bg_width - slider_width
                #     print(f'计算得到拖动距离：{target_distance}px')
                # else:
                target_distance = 308
                print(f'使用默认拖动距离：{target_distance}px')

                # 执行拖动
                self.page.actions.hold(slider)
                time.sleep(0.2)

                # 快速拖动前80%
                fast_part = int(target_distance * 0.8)
                moved = 0

                while moved < fast_part:
                    step = random.randint(14, 18)
                    step = min(step, fast_part - moved)
                    self.page.actions.move(offset_x=step, offset_y=random.randint(-1, 1))
                    moved += step
                    time.sleep(0.008)

                # 慢速完成剩余部分
                while moved < target_distance:
                    remaining = target_distance - moved
                    if remaining > 20:
                        step = random.randint(5, 8)
                    elif remaining > 10:
                        step = random.randint(2, 4)
                    else:
                        step = 1

                    self.page.actions.move(offset_x=step, offset_y=random.randint(-1, 1))
                    moved += step
                    time.sleep(0.04)  # 慢速

                self.page.actions.release()
                time.sleep(0.5)

                print(f'拖动完成，总距离：{moved}px')

            # 如果没有弹窗，继续等待SKU表格加载
            print("等待SKU弹窗加载...")
            sku_dialog = 'xpath://div[contains(@class, "sku-sale-info-table")]'
            try:
                self.page.wait.ele_displayed(sku_dialog, timeout=10)
                print("SKU弹窗已出现")
            except:
                print("SKU弹窗未出现")
                # 再次检查是否有弹窗
                if self.check_no_sku_popup():
                    return []
                raise

            # 等待表格出现
            sku_table = 'xpath://div[contains(@class, "sku-sale-info-table")]//table'
            self.page.wait.ele_displayed(sku_table, timeout=10)
            print("SKU表格已出现")

            # 等待加载完成
            self.wait_for_loading_complete()

            # 获取页面HTML并解析
            page_html = self.page.html
            sku_data = self.parse_sku_data_advanced(page_html, url)

            if not sku_data:
                print("警告：未获取到SKU数据，等待3秒后重试...")
                time.sleep(3)
                # 重试前再检查一次弹窗
                if self.check_no_sku_popup():
                    return []
                page_html = self.page.html
                sku_data = self.parse_sku_data_advanced(page_html, url)

            # 打印结果
            if sku_data:
                print(f"✅ 成功获取 {len(sku_data)} 条SKU数据")
                for i, sku in enumerate(sku_data[:5], 1):
                    print(f"  {i}. SKUID: {sku['skuid']}, 名称: {sku['sku_name']}, 价格: {sku['page_price']}")
                if len(sku_data) > 5:
                    print(f"  ... 还有 {len(sku_data) - 5} 条数据")
            else:
                print("⚠️ 未能获取到SKU数据")

            return sku_data

        except Exception as e:
            print(f"跳转到商品页面失败: {e}")
            raise

    def wait_for_loading_complete(self, timeout=20):
        """
        等待所有"加载中"文字消失
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # 检查是否还有"加载中"的文字
                loading_elements = self.page.eles('xpath://td[contains(text(), "加载中")]',timeout=1)
                if not loading_elements:
                    print("✅ 所有SKU数据加载完成")
                    # 再等待一下确保数据稳定
                    time.sleep(1)
                    return True

                # 检查是否有进度条
                progress_bar = self.page.ele('xpath://div[contains(@class, "data-loading-progress")]', timeout=1)
                if progress_bar and progress_bar.is_displayed():
                    time.sleep(0.5)
                    continue

            except Exception as e:
                print(f"检查加载状态时出错: {e}")

            time.sleep(0.5)

        print("⚠️ 等待加载超时")
        return False

    def parse_sku_data_advanced(self, html_content, url):
        """
        解析SKU数据
        """
        try:
            # 先检查HTML中是否包含暂无SKU的提示
            if any(msg in html_content for msg in ["暂无SKU", "该产品暂无SKU", "请刷新重试"]):
                print("HTML内容中包含暂无SKU提示")
                return []

            soup = BeautifulSoup(html_content, 'html.parser')
            sku_dict = {}  # 使用字典，以skuid为键来去重

            # 查找所有表格行
            rows = soup.select('tr.dld-table__row')

            if not rows:
                print("未找到SKU表格行，可能没有SKU数据")
                return []

            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 6:
                    # 提取SKU信息
                    sku_cell = cells[0]
                    info_divs = sku_cell.find_all('div', class_='column-start-start')

                    if info_divs:
                        inner_divs = info_divs[0].find_all('div')
                        if len(inner_divs) >= 2:
                            # SKUID
                            skuid_text = inner_divs[0].get_text(strip=True)
                            sku_match = re.search(r'SKUID:(\d+)', skuid_text)
                            skuid = sku_match.group(1) if sku_match else ''

                            # SKU名称
                            sku_name = inner_divs[1].get_text(strip=True)

                            # 跳过加载中的行
                            if sku_name == '加载中':
                                continue

                            # 折后价格（第3列，索引2）
                            price_text = cells[2].get_text(strip=True)
                            discount_price = None
                            if price_text and price_text not in ['加载中', '暂无数据']:
                                try:
                                    price_text = price_text.replace('¥', '').replace('￥', '').strip()
                                    if price_text:
                                        discount_price = float(price_text)
                                except:
                                    discount_price = price_text

                            # 只有有效的SKU数据才添加，使用skuid作为键去重
                            if skuid or (sku_name and sku_name not in ['加载中', '']):
                                # 如果skuid存在，用它作为键；否则用sku_name作为键
                                key = skuid if skuid else sku_name
                                if key not in sku_dict:
                                    sku_dict[key] = {
                                        "url": url,
                                        'skuid': skuid,
                                        'sku_name': sku_name,
                                        'page_price': discount_price,
                                        "sku_count": 0,
                                        "price_changed": "否",
                                        "name_changed": "否",
                                        "count_changed": "否",
                                    }

            return list(sku_dict.values())

        except Exception as e:
            print(f"解析SKU数据失败: {e}")
            return []


    def run(self,url):
        """测试主函数"""
        # 测试有SKU的商品
        ali = SkuFromPlugin(headless=False)

        try:
            ali.init_browser()
            ali.check_and_login()

            # 测试有SKU的商品
            print("\n" + "=" * 60)
            print("测试有SKU的商品")
            print("=" * 60)
            sku_data = ali.goto_goods(url)
            return sku_data

            # 测试无SKU的商品（如果有的话）
            # print("\n" + "="*60)
            # print("测试无SKU的商品")
            # print("="*60)
            # sku_data = ali.goto_goods('https://detail.1688.com/offer/854259763574.html')

        finally:
            ali.close()


if __name__ == '__main__':
    s=SkuFromPlugin(headless=False)
    s.run('https://detail.1688.com/offer/854259763574.html')
