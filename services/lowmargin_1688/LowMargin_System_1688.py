import requests
from utils.cookie_manager import CookieManager
from datetime import datetime, timedelta
from utils.logger import get_logger
import json
import asyncio
import re


class LowMarginSystem_1688:
    def __init__(self,shop_id,shop_name,job):
        self.shop_name = shop_name
        self.shop_id = shop_id
        self.cookie_manager = CookieManager(job)
        self.headers = {
            "Origin": "https://bi.erp321.com",
            "Referer": "https://bi.erp321.com/app/daas/report/subject/adsorder/sku.aspx?r=0.39618730504138455&___skutype=combinesku",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        }
        self.cookies = None
        self.url = "https://bi.erp321.com/app/daas/report/subject/adsorder/sku.aspx"
        self.logger=get_logger('1688-LowMargin-System')
        # 添加数据获取成功的标志
        self.data_fetched_successfully = False

    def is_cookie_invalid(self, json_data):
        """
        统一判断 cookie 是否失效
        """
        # 请求异常
        if not json_data:
            return True

        # get_info 主动标记
        if json_data.get("msg")=="子系统登录重定向":
            return True

        if not isinstance(json_data, dict):
            return True

        return False

    def get_yesterday(self,fmt="%Y-%m-%d"):
        yesterdaty=(datetime.now() - timedelta(days=1)).strftime(fmt)
        today=datetime.today().strftime(fmt)
        return yesterdaty,today

    def build_callback_param(self,page):
        # 这是基础的表格配置，保持不变的部分
        table_config = '''[{"class":"rpt head index excel","defaultText":"","is_index":true,"text":"","width":"60px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"","index":0},{"class":"rpt head fixed","defaultText":"图片","is_index":false,"text":"图片","width":"50px","alignClass":"left","formatClass":null,"visible":true,"isFixed":true,"id":"pic","index":1},{"class":"rpt head orderby fixed","defaultText":"商品编码","is_index":false,"text":"商品编码","width":"160px","alignClass":"left","formatClass":null,"orderby":"asc","visible":true,"isFixed":true,"id":"商品编码","index":2},{"class":"rpt head fixed","defaultText":"分析","is_index":false,"text":"分析","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":true,"id":"sales_bar","index":3},{"class":"rpt head orderby","defaultText":"款式编码","is_index":false,"text":"款式编码","width":"100px","alignClass":"left","formatClass":null,"orderby":"asc","visible":true,"isFixed":false,"id":"款式编码","index":4},{"class":"rpt head","defaultText":"商品标签","is_index":false,"text":"商品标签","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"商品标签","index":5},{"class":"rpt head","defaultText":"供应商","is_index":false,"text":"供应商","width":"120px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"供应商","index":6},{"class":"rpt head","defaultText":"供应商款号","is_index":false,"text":"供应商款号","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"供应商款号","index":7},{"class":"rpt head","defaultText":"供应商商品编码","is_index":false,"text":"供应商商品编码","width":"170px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"供应商商品编码","index":8},{"class":"rpt head orderby","defaultText":"颜色规格","is_index":false,"text":"颜色规格","width":"120px","alignClass":"left","formatClass":null,"orderby":"asc","visible":true,"isFixed":false,"id":"颜色规格","index":9},{"class":"rpt head orderby","defaultText":"商品简称","is_index":false,"text":"商品简称","width":"170px","alignClass":"left","formatClass":null,"orderby":"asc","visible":true,"isFixed":false,"id":"商品简称","index":10},{"class":"rpt head","defaultText":"商品品牌","is_index":false,"text":"商品品牌","width":"260px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"商品品牌","index":11},{"class":"rpt head","defaultText":"产品分类","is_index":false,"text":"产品分类","width":"120px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"产品分类","index":12},{"class":"rpt head","defaultText":"虚拟分类","is_index":false,"text":"虚拟分类","width":"120px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"虚拟分类","index":13},{"class":"rpt head right amount orderby","defaultText":"成本价","is_index":false,"text":"成本价","width":"120px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"成本价","index":14},{"class":"rpt head  right qty orderby desc active","defaultText":"销售数量","title":"订单商品数量","is_index":false,"text":"销售数量","width":"80px","alignClass":"right","formatClass":"qty","orderby":"desc","orderbyActive":"active","visible":true,"isFixed":false,"id":"销售数量","index":15},{"class":"rpt head  right qty orderby","defaultText":"实发数量","title":"实际发货的商品数量（包括发货后仅退款的数量、不包括未发货仅退款的数量）","is_index":false,"text":"实发数量","width":"80px","alignClass":"right","formatClass":"qty","orderby":"asc","visible":true,"isFixed":false,"id":"实发数量","index":16},{"class":"rpt head right amount orderby desc","defaultText":"实发金额","title":"订单中每个商品实际销售出库数量*(该商品销售金额/该商品销售数量)的汇总，即订单中每个商品的实发数量*商品单价的汇总","is_index":false,"text":"实发金额","width":"120px","alignClass":"right","formatClass":"amount","orderby":"desc","visible":true,"isFixed":false,"id":"实发金额","index":17},{"class":"rpt head  right amount orderby","defaultText":"销售金额","title":"订单实际已付金额","is_index":false,"text":"销售金额","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"销售金额","index":18},{"class":"rpt head  right amount orderby","defaultText":"销售成本","title":"销售数量 * 成本价","is_index":false,"text":"销售成本","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"销售成本","index":19},{"class":"rpt head  right amount","defaultText":"实发成本","title":"实发数量 * 成本价","is_index":false,"text":"实发成本","width":"120px","alignClass":"right","formatClass":"amount","visible":true,"isFixed":false,"id":"实发成本","index":20},{"class":"rpt head  right amount","defaultText":"销售毛利","title":"销售金额 - 销售成本","is_index":false,"text":"销售毛利","width":"100px","alignClass":"right","formatClass":"amount","visible":true,"isFixed":false,"id":"销售毛利","index":21},{"class":"rpt head  right ","defaultText":"销售毛利率","title":"销售毛利 / 销售金额","is_index":false,"text":"销售毛利率","width":"100px","alignClass":"right","formatClass":null,"visible":true,"isFixed":false,"id":"销售毛利率","index":22},{"class":"rpt head  right amount","defaultText":"销售均价","is_index":false,"text":"销售均价","width":"100px","alignClass":"right","formatClass":"amount","visible":true,"isFixed":false,"id":"销售均价","index":23},{"class":"rpt head right qty orderby","defaultText":"退货数量","title":"售后商品数量（包含仅退款）","is_index":false,"text":"退货数量","width":"80px","alignClass":"right","formatClass":"qty","orderby":"asc","visible":true,"isFixed":false,"id":"退货数量","index":24},{"class":"rpt head right qty orderby","defaultText":"实退数量","title":"售后退货到货商品数量","is_index":false,"text":"实退数量","width":"80px","alignClass":"right","formatClass":"qty","orderby":"asc","visible":true,"isFixed":false,"id":"实退数量","index":25},{"class":"rpt head right amount orderby","defaultText":"退货金额","title":"实际应退金额","is_index":false,"text":"退货金额","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"退货金额","index":26},{"class":"rpt head right amount","defaultText":"退货成本","title":"退货数量 * 成本价","is_index":false,"text":"退货成本","width":"100px","alignClass":"right","formatClass":"amount","visible":true,"isFixed":false,"id":"退货成本","index":27},{"class":"rpt head right amount","defaultText":"实退成本","title":"实退数量 * 成本价","is_index":false,"text":"实退成本","width":"120px","alignClass":"right","formatClass":"amount","visible":true,"isFixed":false,"id":"实退成本","index":28},{"class":"rpt head right amount orderby desc","defaultText":"实退金额","title":"实际退款金额（包含平台补贴）","is_index":false,"text":"实退金额","width":"140px","alignClass":"right","formatClass":"amount","orderby":"desc","visible":true,"isFixed":false,"id":"实退金额","index":29},{"class":"rpt head right amount","defaultText":"退货毛利","title":"退货金额 - 退货成本","is_index":false,"text":"退货毛利","width":"100px","alignClass":"right","formatClass":"amount","visible":true,"isFixed":false,"id":"退货毛利","index":30},{"class":"rpt head  right qty orderby","defaultText":"净销量","title":"销售数量 - 退货数量","is_index":false,"text":"净销量","width":"80px","alignClass":"right","formatClass":"qty","orderby":"asc","visible":true,"isFixed":false,"id":"净销量","index":31},{"class":"rpt head  right amount orderby","defaultText":"净销售额","title":"销售金额 - 退货金额","is_index":false,"text":"净销售额","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"净销售额","index":32},{"class":"rpt head  right amount orderby","defaultText":"净销售成本","title":"销售成本 - 退货成本","is_index":false,"text":"净销售成本","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"净销售成本","index":33},{"class":"rpt head  right amount orderby","defaultText":"净销售毛利","title":"销售毛利 - 退货毛利","is_index":false,"text":"净销售毛利","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"净销售毛利","index":34},{"class":"rpt head right amount orderby","defaultText":"运费收入","title":"订单商品分摊后的买家支付的订单运费","is_index":false,"text":"运费收入","width":"80px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"运费收入","index":35},{"class":"rpt head right amount orderby","defaultText":"运费支出","is_index":false,"text":"运费支出","width":"80px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"运费支出","index":36},{"class":"rpt head  right","defaultText":"净毛利率","title":"( 净销售额 - 净销售成本 ) / 净销售额","is_index":false,"text":"净毛利率","width":"80px","alignClass":"right","formatClass":null,"visible":true,"isFixed":false,"id":"净毛利率","index":37},{"class":"rpt head  right amount orderby","defaultText":"其它价格1","title":"取自普通商品资料(其它价格1)","is_index":false,"text":"其它价格1","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"其它价格1","index":38},{"class":"rpt head  right amount orderby","defaultText":"其它价格2","title":"取自普通商品资料(其它价格2)","is_index":false,"text":"其它价格2","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"其它价格2","index":39},{"class":"rpt head  right amount orderby","defaultText":"其它价格3","title":"取自普通商品资料(其它价格3)","is_index":false,"text":"其它价格3","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"其它价格3","index":40},{"class":"rpt head  right amount orderby","defaultText":"其它价格4","title":"取自普通商品资料(其它价格4)","is_index":false,"text":"其它价格4","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"其它价格4","index":41},{"class":"rpt head  right amount orderby","defaultText":"其它价格5","title":"取自普通商品资料(其它价格5)","is_index":false,"text":"其它价格5","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"其它价格5","index":42},{"class":"rpt head","defaultText":"其它属性1","title":"取自普通商品资料(其它属性1)","is_index":false,"text":"其它属性1","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性1","index":43},{"class":"rpt head","defaultText":"其它属性2","title":"取自普通商品资料(其它属性2)","is_index":false,"text":"其它属性2","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性2","index":44},{"class":"rpt head","defaultText":"其它属性3","title":"取自普通商品资料(其它属性3)","is_index":false,"text":"其它属性3","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性3","index":45},{"class":"rpt head","defaultText":"其它属性4","title":"取自普通商品资料(其它属性4)","is_index":false,"text":"其它属性4","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性4","index":46},{"class":"rpt head","defaultText":"其它属性5","title":"取自普通商品资料(其它属性5)","is_index":false,"text":"其它属性5","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性5","index":47},{"class":"rpt head","defaultText":"其它属性6","title":"取自普通商品资料(其它属性6)","is_index":false,"text":"其它属性6","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性6","index":48},{"class":"rpt head","defaultText":"其它属性7","title":"取自普通商品资料(其它属性7)","is_index":false,"text":"其它属性7","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性7","index":49},{"class":"rpt head","defaultText":"其它属性8","title":"取自普通商品资料(其它属性8)","is_index":false,"text":"其它属性8","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性8","index":50},{"class":"rpt head","defaultText":"其它属性9","title":"取自普通商品资料(其它属性9)","is_index":false,"text":"其它属性9","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性9","index":51},{"class":"rpt head","defaultText":"其它属性10","title":"取自普通商品资料(其它属性10)","is_index":false,"text":"其它属性10","width":"100px","alignClass":"left","formatClass":null,"visible":true,"isFixed":false,"id":"其它属性10","index":52},{"class":"rpt head  right amount orderby","defaultText":"基本售价","title":"取自普通商品资料","is_index":false,"text":"基本售价","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":true,"isFixed":false,"id":"基本售价","index":53},{"class":"rpt head orderby rpthide","defaultText":"商品名称","is_index":false,"text":"商品名称","width":"170px","alignClass":"left","formatClass":null,"orderby":"asc","visible":false,"isFixed":false,"id":"商品名称","index":54},{"class":"rpt head right amount orderby rpthide","defaultText":"市场吊牌价","title":"商品资料中维护的市场吊牌价","is_index":false,"text":"市场吊牌价","width":"80px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":false,"isFixed":false,"id":"市场吊牌价","index":55},{"class":"rpt head rpthide","defaultText":"国标码","is_index":false,"text":"国标码","width":"120px","alignClass":"left","formatClass":null,"visible":false,"isFixed":false,"id":"国标码","index":56},{"class":"rpt head right amount orderby rpthide","defaultText":"市场吊牌金额","is_index":false,"text":"市场吊牌金额","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":false,"isFixed":false,"id":"市场吊牌金额","index":57},{"class":"rpt head right qty orderby desc rpthide","defaultText":"价格为零的商品数量","is_index":false,"text":"价格为零的商品数量","width":"150px","alignClass":"right","formatClass":"qty","orderby":"desc","visible":false,"isFixed":false,"id":"价格为零的商品数量","index":58},{"class":"rpt head right amount orderby rpthide","defaultText":"基本金额","title":"取自订单商品的成交金额","is_index":false,"text":"基本金额","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":false,"isFixed":false,"id":"基本金额","index":59},{"class":"rpt head right amount orderby rpthide","defaultText":"已付金额","title":"订单已付金额（按商品明细分摊求和）","is_index":false,"text":"已付金额","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":false,"isFixed":false,"id":"已付金额","index":60},{"class":"rpt head right amount orderby rpthide","defaultText":"优惠金额","is_index":false,"text":"优惠金额","width":"100px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":false,"isFixed":false,"id":"优惠金额","index":61},{"class":"rpt head right amount orderby rpthide","defaultText":"境外运费支出","is_index":false,"text":"境外运费支出","width":"160px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":false,"isFixed":false,"id":"境外运费支出","index":62},{"class":"rpt head right amount orderby rpthide","defaultText":"境外收入总计","is_index":false,"text":"境外收入总计","width":"160px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":false,"isFixed":false,"id":"境外收入总计","index":63},{"class":"rpt head right amount orderby rpthide","defaultText":"境外支出总计","is_index":false,"text":"境外支出总计","width":"160px","alignClass":"right","formatClass":"amount","orderby":"asc","visible":false,"isFixed":false,"id":"境外支出总计","index":64}]'''

        # 排序参数
        sort_param = '{"fld":"销售数量","type":"desc"}'

        # 构建最终的JSON
        callback_data = {
            "Method": "LoadDataToJSON",
            "Args": [
                str(page),  # 第一个参数是页码
                "",  # 第二个参数是空字符串
                sort_param,  # 第三个参数是排序JSON
                table_config  # 第四个参数是表格配置JSON数组
            ],
            "CallControl": "{page}"  # 这里保持原样，或者你也可以传入实际的page值
        }

        # 将Python字典转换为JSON字符串，并进行必要的转义
        return json.dumps(callback_data)

    async def get_VIEWSTATE(self):
        url = "https://bi.erp321.com/app/daas/report/subject/adsorder/sku.aspx"

        response = requests.get(url, headers=self.headers, cookies=self.cookies)

        vie = re.findall(r'<input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="(.*?)" />', response.text)[0]

        return vie

    def get_info(self,page,cookies):
        self.cookies=cookies
        shop_id=self.shop_id
        yesterday,today = self.get_yesterday()

        self.logger.info(f'正在获取{yesterday}的1688的低利润数据')

        search_obj = [
            {"k": "nolabels", "v": "特殊单,统计排除标", "c": "@like", "t": ""},
            {"k": "cost_type", "v": "1", "c": "@=", "t": ""},
            {"k": "A.status", "v": "MERGED,SPLIT", "c": "@!=", "t": ""},
            {"k": "A.shop_id", "v": shop_id, "c": "@=", "t": ""},
            {"k": "A.wms_co_id", "v": "11479246", "c": "@=", "t": ""},
            {"k": "combinesku_type", "v": 2, "c": "@=", "t": ""},
            {"k": "combinesku", "v": 1, "c": "@=", "t": ""},
            {"k": "C.sent_flag", "v": "1", "c": "@=", "t": ""},
            {"k": "is_currency", "v": 0, "c": "@=", "t": ""},
            {"k": "A.send_date", "v": yesterday, "c": ">=", "t": "date"},
            {"k": "export_date_begin", "v": yesterday, "c": ">="},
            {"k": "A.send_date", "v": today, "c": "<", "t": "date"},
            {"k": "export_date_end", "v": today, "c": "<"}
        ]

        # 将search_obj转换为JSON字符串
        search_str = json.dumps(search_obj, ensure_ascii=False)

        VIEW_STATE = self.get_VIEWSTATE()
        try:
            data = {
                "__VIEWSTATE": f"{VIEW_STATE}",
                "__VIEWSTATEGENERATOR": "8C2ED605",
                "search": search_str,
                "dataPageCount": "",
                "search_click": "",
                "__CALLBACKID": "ACall1",
                "__CALLBACKPARAM":self.build_callback_param(page)
            }

            response = requests.post(self.url, headers=self.headers, cookies=self.cookies, data=data)
            print(response.text[:200])
            return response.text

        except Exception as e:
            self.logger.error('请求解析错误:', e)
            print('请求解析错误:', e)

    # ---------处理数据格式的工具---------
    def percent_str_to_ratio(self,s: str) -> float:
        if not s:
            return 0.0
        return float(s.replace('%', '').strip()) / 100

    def safe_load_json(self,res_text):
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
                return return_data
            except json.JSONDecodeError as e:
                self.logger.error(f"ReturnValue JSON解析失败: {e}")
                print(f"ReturnValue JSON解析失败: {e}")

                # 尝试不同的修复策略
                # 策略1: 使用 ast.literal_eval 作为备选方案
                import ast
                try:
                    return_data = ast.literal_eval(return_value_str)
                    return return_data
                except:
                    # 策略2: 使用简单的字符串替换
                    # 修复未转义的双引号
                    return_value_str = return_value_str.replace('"', '\"')
                    # 处理Unicode转义
                    return_value_str = return_value_str.encode('unicode_escape').decode('utf-8')

                    try:
                        return_data = json.loads(return_value_str)
                        return return_data
                    except:
                        self.logger.error("所有修复尝试都失败")
                        return []



        except Exception as e:
            self.logger.error(f"解析数据时发生错误: {e}")
            return []

    # ---------解析数据--------
    def parse_data(self, json_data):
        low_list = []

        datas = json_data.get("datas", [])
        if not isinstance(datas, list):
            return low_list

        for i in datas[:-1]:
            item = {
                # "商品编码": i.get("商品编码", ""),
                "商品名称": i.get("商品名称", ""),
                "销售均价": i.get("销售均价", 0),
                "成本价": i.get("成本价", 0),
                "销售数量": i.get("销售数量", 0),
            }

            rate = self.percent_str_to_ratio(i.get("销售毛利率"))
            item["销售毛利率"] = i.get("销售毛利率")

            if rate < 0.13:
                low_list.append(item)

        return low_list

    """实现翻页，获取所有页面的数据"""

    async def get_all_page(self):
        page = 1
        max_page = 10
        max_retry = 3
        all_items = []

        # 重置标志
        self.data_fetched_successfully = False

        while page <= max_page:
            json_data = None
            try:
                self.logger.info(f'正在爬取---{self.shop_name}---第{page}页的数据')

                for attempt in range(3):
                    try:
                        # 获取当前页的数据
                        cookies = await self.cookie_manager.get_auth()

                        res_text = self.get_info(page, cookies)

                        # 检查响应是否为空
                        if not res_text:
                            self.logger.warning(f"第{page}页返回空响应")
                            raise PermissionError("返回空响应")

                        json_data = self.safe_load_json(res_text)

                        # ⭐ 核心：统一失效判断
                        if self.is_cookie_invalid(json_data):
                            raise PermissionError("cookie 已失效或接口异常")

                        # 如果有有效数据，标记成功
                        if json_data and isinstance(json_data, dict) and json_data.get("datas"):
                            self.data_fetched_successfully = True

                        # 成功直接跳出 retry
                        break
                    except PermissionError as e:
                        self.logger.warning(
                            f"[{self.shop_name}] 第 {page} 页 cookie 失效，刷新中（{attempt}/{max_retry}）"
                        )
                        await self.cookie_manager.refresh()
                        await asyncio.sleep(2)

                    except Exception as e:
                        self.logger.error(
                            f"[{self.shop_name}] 第 {page} 页请求异常（{attempt}/{max_retry}）：{e}"
                        )
                        await self.cookie_manager.refresh()
                        await asyncio.sleep(2)

                # ---------- retry 全失败 ----------
                if not json_data:
                    self.logger.error(
                        f"[{self.shop_name}] 第 {page} 页多次失败，终止任务"
                    )
                    break

                # 解析数据
                items = self.parse_data(json_data)
                all_items.extend(items)

                # ---------- 没数据，结束 ----------
                datas = json_data.get("datas", [])
                if not isinstance(datas, list):
                    return []
                if len(datas) < 200:
                    self.logger.info('已经达到最后一页了')
                    break

                page += 1

            except Exception as e:
                self.logger.info("用户中断爬取")
                break
        return all_items

def format_low_margin_report(
    low_items: list,
    shop_name: str,
    date_str: str = None,
    threshold: float = 0.13
):
    date_str = date_str or datetime.now().strftime("%m-%d")

    # ---------- 过滤有效低利润 ----------
    valid_items = [
        i for i in low_items
        if i.get("销售毛利率")
    ]

    # ---------- 没有低利润 ----------
    if not valid_items:
        return (
            f"✅ {shop_name} {date_str} 低利润监控\n\n"
            f"今日暂无销售毛利率 < {int(threshold*100)}% 的 SKU"
        )

    # ---------- 有低利润 ----------
    lines = []
    lines.append(f"📉 {shop_name} {date_str} 低利润 SKU（<{int(threshold*100)}%）\n")
    lines.append("商品名 / 销售均价 / 成本价 / 销售数量 / 销售毛利率")
    lines.append("-" * 50)

    for i in valid_items:
        lines.append(
            f"{i.get('商品名称','')[:12]:<12} / "
            f"{i.get('销售均价',0):>6.2f} / "
            f"{i.get('成本价',0):>6.2f} / "
            f"{int(i.get('销售数量',0)):>4} / "
            f"{i.get('销售毛利率')}"
        )

    return "\n".join(lines)

def get_yesterday(fmt="%Y-%m-%d"):
    yesterdaty=(datetime.now() - timedelta(days=1)).strftime(fmt)
    # today=datetime.today().strftime(fmt)
    return yesterdaty


from utils.dingtalk_bot import ding_bot_send  # 假设这是钉钉发送函数

if __name__ == '__main__':
    shop_id_list = {
        "嬉游记": "12426494",
        "俏娃": "12430140"
    }

    # 标记是否有成功的店铺
    has_successful_shop = False
    # 记录失败信息
    failure_messages = []
    # 存储每个店铺的结果对象
    shop_instances = []

    for name, id in shop_id_list.items():
        try:
            l = LowMarginSystem_1688(id, name, 'lowmargin_1688')
            shop_instances.append(l)  # 保存实例以便后续检查
            low_items = asyncio.run(l.get_all_page())

            # 判断是否成功获取到数据
            if l.data_fetched_successfully:
                has_successful_shop = True
                text = format_low_margin_report(
                    low_items=low_items,
                    shop_name=name,
                    date_str=get_yesterday()
                )
                print(text)
                print("-" * 80)
            else:
                failure_messages.append(f"{name}: 登录失败或无法获取数据（所有页面均无有效响应）")

        except Exception as e:
            failure_messages.append(f"{name}: {str(e)}")
            print(f"处理店铺 {name} 时发生错误: {e}")

    # 如果没有一个店铺成功，发送钉钉通知
    if not has_successful_shop:
        date_str = get_yesterday()
        message = f"⚠️ 1688低利润数据获取失败告警\n\n"
        message += f"日期: {date_str}\n"
        message += "失败详情:\n"
        for msg in failure_messages:
            message += f"- {msg}\n"
        message += "\n请检查登录状态或系统配置！"

        # 发送钉钉通知
        ding_bot_send('me', message)
        print("\n" + message)  # 同时在控制台输出