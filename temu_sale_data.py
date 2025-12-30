import requests
import json
import time
import os
import csv
from datetime import datetime
from logger_config import SimpleLogger
from login import refresh


class Temu:
    def __init__(self):
        self.headers = {
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://ww.erp321.com',
    'priority': 'u=1, i',
    'referer': 'https://ww.erp321.com/app/wms/crossborder/deliveryware/Temu/SalesStockManager.aspx',
    'sec-ch-ua': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0',
    'x-requested-with': 'XMLHttpRequest',
    # 'cookie': 'jt.pagesize=.-ISIJN._500%3D.-A3FFOB._500; initselectwmsasync22082479=1766554328622; _ati=8897840642886; 3AB9D23F7A4B3CSS=jdd03R4I2AN3DL6EWY3FHH5TEOKN3T34WSFFQEPFYR2Q6QO3H4MK3UBUVFQTRHGQBXPRWULSSXWDHIFJPPVZFWJR6ODXSHYAAAAM2JDCUAOYAAAAACTHRMV6VJLJT6EX; u_ssi=; tmp_gray=1; u_shop=0; _gi=-2; jump_env=ww; isLogin=true; j_d_3=; v_d_144=; 3AB9D23F7A4B3C9B=R4I2AN3DL6EWY3FHH5TEOKN3T34WSFFQEPFYR2Q6QO3H4MK3UBUVFQTRHGQBXPRWULSSXWDHIFJPPVZFWJR6ODXSHY; u_lastLoginType=ap; u_isTPWS=0; u_name=RPA%e4%b8%93%e7%94%a8; u_lid=18165643805; u_co_name=%e4%b9%90%e5%85%88%e7%9f%a5%e8%b4%b8%e6%98%93; u_drp=-1; u_cid=134098902853971568; u_r=12%2c13%2c14%2c15%2c17%2c18%2c22%2c23%2c27%2c28%2c29%2c30%2c31%2c32%2c33%2c34%2c35%2c36%2c39%2c40%2c41; u_sso_token=CS@4791df06e1e94cc79ca98e976277178d; u_id=22082479; u_co_id=11416191; p_50=3ACBEDED2A46666B7E21181518A752D9639010422853979168%7c11416191; u_env=ww; jump_isgray=1; acw_tc=1a0c63d717665542500266461ea0f070c805a9a6f9902b74fae842708b5c20; u_json=%7b%22t%22%3a%222025-12-24+13%3a30%3a50%22%2c%22co_type%22%3a%22%e6%a0%87%e5%87%86%e5%95%86%e5%ae%b6%22%2c%22proxy%22%3anull%2c%22ug_id%22%3a%22%22%2c%22dbc%22%3a%221339%22%2c%22tt%22%3a%2216%22%2c%22apps%22%3a%221.4.14.150.152%22%2c%22pwd_valid%22%3a%220%22%2c%22ssi%22%3a%22%22%2c%22sign%22%3a%224592351.DF72C18B622842F8B356B1F04BC2C0A3%2c24929c5f8fd258351749c3d2a48751f3%22%7d; tfstk=g-FSgW0jD3x5Z7QRv372cyvN5LhQRZ5ZRegLSydyJbhJvHnbf2oE2vhYhkEmU0FQ-BgLJkboYaX4rzcn9GSZO1zurJWfwYjq9wUxT2swgrf0rzcHohS4en4lO7Ur1DhLvjKxW4hpyXEpkxnm-2K-JQQbkmmxyQdKvELx-2ipvkhdlrnmJXnLvYQbkm0K9DhW-FgCF0457syJAH-oqznX9BFAtYi_ypRp9SgSFWU-czzzG4MSXvKr1UN80yFzZxXvhX44CkwLXiJr2Rg_cAy5fQZ_q2UIJu1M5mF7RSMu3FATlvi7H7HXJBUSiDMadusWE0ezcA0xHe1iuloYr7ef-nU8b0G-kx5O5rHL37DgaidS6R4raJEO0Lo8C2IzTCo_c3vBlAAKlc7flpvn4BqA8rHw-eH-o4MVlZt8KY3mlc7flpvneq0S7Z_X2Jf..; _pdd_page_code=f9f9d7a9b1b14785e54e3085eb964e47f8def16f8f7b; _pati=M5I0Ki7tcgoDDEHHaxG5WnXXOE5jzyd5; _pati_v=v2',
}
        self.cookies=None
        self.params = {
            'ts___': '1765433527407',
            'am___': 'LoadDataToJSON',
        }
        self.logger = SimpleLogger('temu_sale_data')

    def get_cookies(self):
        with open('./data/cookies.json','r') as f:
            cookies=json.loads(f.read())
        return cookies

    """获取指定页面的数据"""
    def get_info(self, page):
        try:

            data = {
                '__VIEWSTATE': '/wEPDwUKMTk2MDI4ODg4M2RkfJW8R/0sdB3alhEy2AeDro/swmc=',
                '__VIEWSTATEGENERATOR': '96FE7ACB',
                'pagetype': 'Temu',
                'sales_qty': 'today',
                '_jt_page_count_enabled': 'true',
                '_jt_page_size': '500',
                '__CALLBACKID': 'JTable1',
                '__CALLBACKPARAM': f'{{"Method":"LoadDataToJSON","Args":["{str(page)}","[]","{{}}"]}}',

            }

            url = 'https://ww.erp321.com/app/wms/crossborder/deliveryware/Temu/SalesStockManager.aspx'
            response = requests.post(url=url, params=self.params,headers=self.headers, cookies=self.cookies,data=data)
            return response.text
        except Exception as e:
            self.logger.error('请求解析错误:', e)
            print('请求解析错误:', e)

    def parse_data(self, res_text):
        # 解析数据
        try:
            # 首先去除开头的 "0|"
            if res_text.startswith('0|'):
                res_text = res_text[2:]

            # 尝试直接解析整个JSON
            try:
                response_data = json.loads(res_text)
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON解析失败: {e}")
                print(f'JSON解析失败: {e}')

                # 尝试修复常见的JSON格式问题
                # 1. 处理可能的转义字符问题
                res_text = res_text.replace('\\', '\\\\')
                # 2. 尝试再次解析
                try:
                    response_data = json.loads(res_text)
                except:
                    self.logger.error("修复后仍然解析失败")
                    print("修复后仍然解析失败")
                    return []

            # 获取ReturnValue
            return_value_str = response_data.get('ReturnValue', '{}')

            # 尝试解析ReturnValue
            try:
                return_data = json.loads(return_value_str)
            except json.JSONDecodeError as e:
                self.logger.error(f"ReturnValue JSON解析失败: {e}")
                print(f"ReturnValue JSON解析失败: {e}")

                # 尝试不同的修复策略
                # 策略1: 使用 ast.literal_eval 作为备选方案
                import ast
                try:
                    return_data = ast.literal_eval(return_value_str)
                except:
                    # 策略2: 使用简单的字符串替换
                    # 修复未转义的双引号
                    return_value_str = return_value_str.replace('"', '\"')
                    # 处理Unicode转义
                    return_value_str = return_value_str.encode('unicode_escape').decode('utf-8')

                    try:
                        return_data = json.loads(return_value_str)
                    except:
                        self.logger.error("所有修复尝试都失败")
                        return []

            data_list = return_data.get('datas', [])

        except Exception as e:
            self.logger.error(f"解析数据时发生错误: {e}")
            return []

        last_product_name = ''  # 用于保存上一个商品名称
        items = []

        for i in data_list:
            item = {}
            item['平台'] = 'TEMU'
            item['店铺'] = i.get('shop_name', '')
            item['sku'] = i.get('sku_ext_code', '')

            # 处理商品名称：如果当前商品名称为空，使用上一个非空名称
            current_name = i.get('product_name', '')
            if current_name and current_name.strip():  # 如果当前名称不为空
                item['商品名称'] = current_name
                last_product_name = current_name  # 更新上一个商品名称
            elif last_product_name:  # 如果当前名称为空，但之前有保存过名称
                item['商品名称'] = last_product_name
            else:  # 如果这是第一个且名称为空
                item['商品名称'] = ''

            item['抓取数据日期'] = int(time.time() * 1000)
            item['今日销量'] = i.get('today_sale_volume', 0)
            item['近7天销量'] = i.get('last_seven_days_sale_volume', 0.0)
            item['近30天销量'] = i.get('last_thirty_days_sale_volume', 0.0)
            item['平台库存'] = i.get('warehouse_inventory_num', 0)
            item['在途库存'] = i.get('wait_receive_num', 0)

            items.append(item)
        return items

    """批量保存数据到CSV文件"""

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
        filename = f"./data/data/temu_sale_{current_date}.csv"

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

    """实现翻页，获取所有页面的数据"""

    def get_all_page(self):
        page = 1
        max_page = 100
        total_items = 0

        while page <= max_page:
            print(f'🔍 正在爬取第{page}页的数据')
            self.logger.info(f'正在爬取第{page}页的数据')

            retry = 0
            while retry < 3:  # 最多尝试 2 次（第一次失败后 refresh）
                try:
                    self.cookies = self.get_cookies()

                    res_text = self.get_info(page)

                    # 1️⃣ res_text 为空，直接触发 refresh
                    if not res_text or not res_text.strip():
                        raise ValueError('res_text 为空')

                    items = self.parse_data(res_text)

                    # 2️⃣ 解析后无数据，也视为异常
                    if not items:
                        raise ValueError('解析后数据为空，可能登录失效')

                    # ===== 正常流程 =====
                    print(f"📊 第{page}页获取到{len(items)}条数据")
                    total_items += len(items)

                    header = [
                        '平台', '店铺', '商品名称', 'sku', '抓取数据日期',
                        '今日销量', '近7天销量', '近30天销量', '平台库存', '在途库存'
                    ]
                    self.save_batch(items, header)

                    # 最后一页判断
                    if len(items) < 500:
                        print(f"✅ 第{page}页为最后一页，共获取{total_items}条")
                        return

                    break  # 成功，跳出 retry 循环

                except Exception as e:
                    retry += 1
                    print(f"⚠️ 第{page}页第{retry}次失败：{e}")
                    self.logger.warning(f"第{page}页第{retry}次失败：{e}")

                    if retry == 1:
                        print("🔄 尝试 refresh 登录态...")
                        refresh()  # 👈 关键
                        time.sleep(5)
                    else:
                        print(f"❌ 第{page}页重试失败，终止程序")
                        return

            page += 1
            time.sleep(2)


def temu_run():
    temu = Temu()
    temu.get_all_page()


if __name__ == '__main__':
    temu_run()
