import requests
import hashlib
import time
import json
import os
from login import SimpleLogin
import csv
from datetime import datetime
from logger_config import SimpleLogger

class SMT_Good:
    def __init__(self, shop_name, force_relogin=False):
        self.shop_name = shop_name
        self.cookies = None
        self.headers = {
            'origin': 'https://csp.aliexpress.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'referer': 'https://csp.aliexpress.com/',
            'content-type': 'application/x-www-form-urlencoded',
            'accept': 'application/json',
            'accept-language': 'zh-CN,zh;q=0.9',
        }
        self.url = 'https://seller-acs.aliexpress.com/h5/mtop.ae.scitem.read.pagequery/1.0/'
        self.token = ''
        self.totalpage = 1
        self.logger = SimpleLogger(name='SMT_GOODS')  # 添加日志记录器

        # 加载账号信息
        self.account_data = self.load_account_info()

        if force_relogin:
            self.force_login()
        else:
            self.load_cookies_from_file()

    def load_account_info(self):
        """从文件加载账号信息"""
        try:
            with open('data/smt_accounts.json', 'r', encoding='utf-8') as f:
                accounts = json.load(f)

            if self.shop_name not in accounts:
                raise ValueError(f"未找到门店: {self.shop_name}")

            return accounts[self.shop_name]

        except Exception as e:
            print(f"加载账号信息失败: {e}")
            self.logger.error(f"加载账号信息失败: {e}")
            return None

    def load_cookies_from_file(self):
        """从文件加载cookies"""
        try:
            filename = f"./data/{self.shop_name}_cookies.json"
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cookies = data.get('cookies_dict', {})

                    # 从cookies中提取token
                    m_h5_tk = self.cookies.get('_m_h5_tk', '')
                    self.token = m_h5_tk.split('_')[0] if '_' in m_h5_tk else ''

                    print(f"✅ 从文件加载cookies成功: {filename}")
                    self.logger.info(f"从文件加载cookies成功: {filename}")
                    print(f"提取的token: {self.token}")
                    self.logger.info(f"提取的token: {self.token}")
                    return True
            else:
                print(f"⚠️ Cookie文件不存在: {filename}")
                self.logger.warning(f"Cookie文件不存在: {filename}")
                return False

        except Exception as e:
            print(f"加载cookies文件失败: {e}")
            self.logger.error(f"加载cookies文件失败: {e}")
            return False

    def check_cookies_valid(self):
        """检查cookies是否有效"""
        if not self.cookies or '_m_h5_tk' not in self.cookies:
            print("❌ Cookies为空或缺少关键字段")
            self.logger.error("Cookies为空或缺少关键字段")
            return False

        # 测试获取第一页数据
        test_result = self.get_info(1, test_mode=True)
        if test_result and test_result.get('ret') and isinstance(test_result['ret'], list):
            ret_code = test_result['ret'][0]
            if 'SUCCESS' in ret_code:
                print("✅ Cookies验证有效")
                self.logger.info("Cookies验证有效")
                return True
            else:
                print(f"❌ Cookies验证失败: {ret_code}")
                self.logger.error(f"Cookies验证失败: {ret_code}")
                return False
        else:
            print("❌ Cookies验证失败: 无法获取数据")
            self.logger.error("Cookies验证失败: 无法获取数据")
            return False

    def _delete_old_cookie_file(self):
        """删除旧的cookie文件"""
        filename = f"./data/{self.shop_name}_cookies.json"
        try:
            if os.path.exists(filename):
                os.remove(filename)
                print(f"🗑️ 已删除旧的cookie文件: {filename}")
                self.logger.info(f"已删除旧的cookie文件: {filename}")
                return True
            else:
                print(f"ℹ️ 旧的cookie文件不存在: {filename}")
                self.logger.info(f"旧的cookie文件不存在: {filename}")
                return False
        except Exception as e:
            print(f"❌ 删除旧cookie文件失败: {e}")
            self.logger.error(f"删除旧cookie文件失败: {e}")
            return False

    def force_login(self):
        """强制重新登录"""
        print(f"🔐 强制重新登录: {self.shop_name}")
        self.logger.info(f"强制重新登录: {self.shop_name}")

        # 重置所有状态
        self.cookies = None
        self.token = ''

        # 先删除旧的cookie文件
        self._delete_old_cookie_file()

        if not self.account_data:
            print("❌ 无法获取账号信息，登录失败")
            self.logger.error("无法获取账号信息，登录失败")
            return False

        login_client = SimpleLogin(
            shop_name=self.shop_name,
            account=self.account_data['account'],
            password=self.account_data['password'],
            channelId=self.account_data['channelId']
        )

        try:
            if login_client.login():
                # 等待一小段时间确保cookie生效
                time.sleep(2)

                self.cookies = login_client.cookies.copy()  # 使用copy防止引用问题

                # 强制保存cookies
                login_client.save_cookies()

                # 重新从文件加载以确保一致性
                self.load_cookies_from_file()

                # 从cookies中提取token
                m_h5_tk = self.cookies.get('_m_h5_tk', '')
                self.token = m_h5_tk.split('_')[0] if '_' in m_h5_tk else ''

                print(f"✅ 重新登录成功，提取的token: {self.token}")
                print(f"✅ 当前cookies: {list(self.cookies.keys())}")
                self.logger.info(f"重新登录成功，提取的token: {self.token}")

                # 验证新token是否有效
                if self.token:
                    test_result = self.get_info(1, test_mode=True)
                    if test_result and test_result.get('ret') and 'SUCCESS' in test_result['ret'][0]:
                        print("✅ 新token验证通过")
                        return True
                    else:
                        print("❌ 新token验证失败")
                        return False
                return True
            else:
                print("❌ 重新登录失败")
                self.logger.error("重新登录失败")
                return False

        except Exception as e:
            print(f"❌ 登录过程异常: {e}")
            self.logger.error(f"登录过程异常: {e}")
            return False
        finally:
            login_client.close()

    def auto_login_if_needed(self):
        """自动登录（如果需要）"""
        # 先尝试从文件加载cookies
        if not self.cookies:
            self.load_cookies_from_file()

        # 检查cookies是否有效
        if self.cookies and self.check_cookies_valid():
            return True
        else:
            print("🔄 Cookies失效，正在重新登录...")
            self.logger.warning("Cookies失效，正在重新登录...")
            return self.force_login()

    def get_md5(self, token, timestamp, app_key, data_str):
        """生成签名"""
        text = f"{token}&{timestamp}&{app_key}&{data_str}"
        print(f"签名字符串: {text}")
        self.logger.debug(f"签名字符串: {text}")

        md5_hash = hashlib.md5()
        md5_hash.update(text.encode('utf-8'))
        return md5_hash.hexdigest()

    def get_info(self, page, test_mode=False):
        # 在获取信息前检查是否需要重新登录
        if not test_mode and not self.auto_login_if_needed():
            print("❌ 登录失败，无法获取数据")
            self.logger.error("登录失败，无法获取数据")
            return None

        timestamp = int(time.time() * 1000)
        app_key = '30267743'
        channelId = self.account_data['channelId']

        # 构造data参数
        data_dict = {
            "pageIndex": page,
            "pageSize": 20,
            "channelId": f"{channelId}"
        }
        data_str = json.dumps(data_dict, separators=(',', ':'))

        # 生成签名
        sign = self.get_md5(self.token, timestamp, app_key, data_str)

        params = {
            'jsv': '2.7.2',
            'appKey': app_key,
            't': str(timestamp),
            'sign': sign,
            'v': '1.0',
            'timeout': '30000',
            'H5Request': 'true',
            'url': 'mtop.ae.scitem.read.pagequery',
            'params': '[object Object]',
            '__channel-id__': f'{channelId}',
            'api': 'mtop.ae.scitem.read.pagequery',
            'type': 'originaljson',
            'dataType': 'json',
            'valueType': 'original',
            'x-i18n-regionID': 'AE',
            'data': data_str,
        }

        try:
            response = requests.post(
                url=self.url,
                cookies=self.cookies,
                headers=self.headers,
                params=params
            )
            print(response.text)
            self.logger.debug(f"响应内容: {response.text}")

            if test_mode:
                # 测试模式下只返回状态信息
                return response.json() if response.status_code == 200 else None

            print(f"响应状态码: {response.status_code}")
            self.logger.info(f"响应状态码: {response.status_code}")

            if response.status_code == 200:
                result = response.json()
                # 检查是否token失效
                if result.get('ret') and isinstance(result['ret'], list):
                    ret_code = result['ret'][0]
                    if 'FAIL_SYS_TOKEN_EMPTY' in ret_code or 'FAIL_SYS_SESSION_EXPIRED' in ret_code:
                        print("⚠️ Token失效，正在重新登录...")
                        self.logger.warning("Token失效，正在重新登录...")
                        if self.force_login():
                            # 重新尝试获取数据
                            return self.get_info(page)
                        else:
                            return None
                return result
            else:
                print(f"请求失败，状态码: {response.status_code}")
                self.logger.error(f"请求失败，状态码: {response.status_code}")
                return None

        except Exception as e:
            print(f"请求异常: {e}")
            self.logger.error(f"请求异常: {e}")
            return None

    def parse_data(self, page):
        """解析一页的数据"""
        print(f"\n📄 正在获取第 {page} 页数据...")
        self.logger.info(f"正在获取第 {page} 页数据...")
        data_json = self.get_info(page)
        if not data_json:
            print(f"❌ 第 {page} 页获取数据失败")
            self.logger.error(f"第 {page} 页获取数据失败")
            return False

        # 检查是否有错误
        print(data_json)
        self.logger.debug(f"第 {page} 页响应数据: {data_json}")

        if data_json.get('ret') and isinstance(data_json['ret'], list):
            ret_code = data_json['ret'][0]
            if 'SUCCESS' not in ret_code:
                print(f"❌ 第 {page} 页API返回错误: {data_json.get('ret', [])}")
                self.logger.error(f"第 {page} 页API返回错误: {data_json.get('ret', [])}")
                return False

        # 获取数据
        if 'data' in data_json:
            if 'totalPages' in data_json['data']:
                total_page = data_json['data']['totalPages']
                self.totalpage = total_page
                print(f'📊 总页数: {total_page}')
                self.logger.info(f'总页数: {total_page}')

            if 'data' in data_json['data'] and data_json['data']['data']:
                items_list = []
                for i in data_json['data']['data']:
                    item = {}
                    item['货号ID'] = i['scitemId']
                    print(item['货号ID'])

                    # 多层安全获取 skuOuterId
                    sku = ''
                    item_sku = i.get('items','')
                    if item_sku and len(item_sku) > 0:
                        sku = item_sku[0].get('skuOuterId', '')
                    item['sku'] = sku
                    # print(f'获取到的数据为:',item)
                    items_list.append(item)
                    print(f"✅ 找到商品: 货号ID={item['货号ID']}, sku={item['sku']}")
                    # self.logger.info(f"找到商品: 货号ID={item['货号ID']}, sku={item['sku']}")

                # 批量保存数据
                if items_list:
                    header = ['货号ID', 'sku']
                    self.save_batch(items_list, header)
                    print(f"✅ 第 {page} 页成功保存 {len(items_list)} 条记录")
                    self.logger.info(f"第 {page} 页成功保存 {len(items_list)} 条记录")
                    return True
                else:
                    print(f"⚠️ 第 {page} 页没有数据")
                    self.logger.warning(f"第 {page} 页没有数据")
                    return False
            else:
                print(f"⚠️ 第 {page} 页没有找到商品数据")
                self.logger.warning(f"第 {page} 页没有找到商品数据")
                return False
        else:
            print(f"❌ 第 {page} 页响应中没有data字段")
            self.logger.error(f"第 {page} 页响应中没有data字段")
            return False

    def save_batch(self, items, header):
        """批量保存数据到CSV文件"""
        data_dir = "./data/data"
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir)
                print(f"📁 创建目录: {data_dir}")
                self.logger.info(f"创建目录: {data_dir}")
            except Exception as e:
                print(f"❌ 创建目录失败: {e}")
                self.logger.error(f"创建目录失败: {e}")
                return

        # 获取当前年月日，格式为 YYYYMMDD
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"./data/data/{self.shop_name}_goods_{current_date}.csv"

        # 检查文件是否存在
        file_exists = os.path.exists(filename)

        # 使用追加模式写入
        with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
            f_csv = csv.DictWriter(f, fieldnames=header)
            if not file_exists:
                f_csv.writeheader()
            f_csv.writerows(items)

        print(f"💾 已保存到文件: {filename}")
        self.logger.info(f"已保存到文件: {filename}")

    def get_all_page(self):
        """获取所有页的数据"""
        print(f"🚀 开始获取店铺 {self.shop_name}---货品管理--- 的所有商品数据...")
        self.logger.info(f"开始获取店铺 {self.shop_name} 的所有商品数据...")

        # 先获取第一页以获取总页数
        success = self.parse_data(1)
        if not success:
            print("❌ 第一页获取失败，停止获取")
            self.logger.error("第一页获取失败，停止获取")
            return False

        # 获取总页数
        total_pages = self.totalpage
        print(f"📊 需要获取的总页数: {total_pages}")
        self.logger.info(f"需要获取的总页数: {total_pages}")

        # 如果只有一页，直接返回
        if total_pages <= 1:
            print("✅ 数据获取完成，只有1页数据")
            self.logger.info("数据获取完成，只有1页数据")
            return True

        # 从第二页开始获取
        for page in range(2, total_pages + 1):
            print(f"\n{'=' * 50}")
            self.logger.info(f"{'=' * 50}")
            print(f"📄 正在获取第 {page}/{total_pages} 页数据...")
            self.logger.info(f"正在获取第 {page}/{total_pages} 页数据...")

            # 解析当前页数据
            success = self.parse_data(page)
            if not success:
                print(f"❌ 第 {page} 页获取失败，继续下一页")
                self.logger.error(f"第 {page} 页获取失败，继续下一页")
                continue

            # 添加延迟，避免请求过快
            if page < total_pages:
                delay = 1  # 1秒延迟
                print(f"⏳ 等待 {delay} 秒后继续下一页...")
                self.logger.info(f"等待 {delay} 秒后继续下一页...")
                time.sleep(delay)

        print(f"\n{'=' * 50}")
        self.logger.info(f"{'=' * 50}")
        print(f"🎉 所有数据获取完成！")
        self.logger.info("所有数据获取完成！")

        return True

    def run(self):
        """运行主程序"""
        print(f"\n{'=' * 50}")
        self.logger.info(f"{'=' * 50}")
        self.logger.info(f"🚀 开始爬取 {self.shop_name}---货品管理--- 的数据")
        """主运行函数，优化翻页逻辑"""
        print(f"🚀 开始爬取 {self.shop_name}---货品管理--- 的数据")
        print(f"{'=' * 50}")
        self.logger.info(f"{'=' * 50}")

        # 检查登录状态
        if not self.auto_login_if_needed():
            print("❌ 登录失败，程序终止")
            self.logger.error("登录失败，程序终止")
            return False

        # 获取所有页数据
        return self.get_all_page()


if __name__ == '__main__':
    # shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
    shop_name_list = ['SMT202']
    for index, shop_name in enumerate(shop_name_list):
        print(f"\n{'=' * 60}")
        print(f"🛍️ 开始处理店铺 {index + 1}/{len(shop_name_list)}: {shop_name}")
        print(f"{'=' * 60}")

        try:
            # 每个店铺使用独立的实例
            s = SMT_Good(shop_name)

            # 添加店铺间的延迟，避免频繁登录
            if index > 0:
                delay = 3  # 店铺间等待3秒
                print(f"⏳ 等待 {delay} 秒后处理下一个店铺...")
                time.sleep(delay)

            # 运行爬虫
            success = s.run()

            if not success:
                print(f"❌ 店铺 {shop_name} 处理失败")
                # 可以记录失败日志，继续下一个店铺
                continue

        except Exception as e:
            print(f"❌ 处理店铺 {shop_name} 时发生异常: {e}")
            continue

        # 清理资源（如果对象有close或cleanup方法）
        if hasattr(s, 'close'):
            s.close()

        # 强制垃圾回收
        import gc

        gc.collect()

    print("\n🎉 所有店铺处理完成！")