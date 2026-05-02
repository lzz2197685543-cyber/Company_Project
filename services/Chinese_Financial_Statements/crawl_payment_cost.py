import asyncio
import pprint

from core.base_client import BaseClient
import json
from datetime import datetime, timedelta
from calendar import monthrange


class PaymentCostClient(BaseClient):
    def __init__(self, job, shop_id, start_date:str, end_date:str):
        super().__init__(job)
        self.shop_id = shop_id
        self.start_date = start_date
        self.end_date = end_date

    def get_visible_columns(self) -> list:
        """获取可见列配置"""
        return [
            {"id": "内部订单号", "visible": True},
            {"id": "标记多标签", "visible": True},
            {"id": "售后单号", "visible": True},
            {"id": "订单类型", "visible": True},
            {"id": "线上订单号", "visible": True},
            {"id": "平台外部订单号", "visible": True},
            {"id": "订单状态", "visible": True},
            {"id": "发货仓", "visible": True},
            {"id": "bu_id", "visible": True},
            {"id": "分销商", "visible": True},
            {"id": "店铺", "visible": True},
            {"id": "买家账号", "visible": True},
            {"id": "buyer_id", "visible": True},
            {"id": "buyer_message", "visible": True},
            {"id": "卖家备注", "visible": True},
            {"id": "订单日期", "visible": True},
            {"id": "发货日期", "visible": True},
            {"id": "付款日期", "visible": True},
            {"id": "确认收货日期", "visible": True},
            {"id": "供销支付时间", "visible": True},
            {"id": "省", "visible": True},
            {"id": "市", "visible": True},
            {"id": "区县", "visible": True},
            {"id": "订单快递公司", "visible": True},
            {"id": "订单快递单号", "visible": True},
            {"id": "售后快递公司", "visible": True},
            {"id": "售后快递单号", "visible": True},
            {"id": "收货人", "visible": True},
            {"id": "售后登记日期", "visible": True},
            {"id": "售后确认日期", "visible": True},
            {"id": "售后进仓日期", "visible": True},
            {"id": "售后进仓单号", "visible": True},
            {"id": "收货仓", "visible": True},
            {"id": "售后分类", "visible": True},
            {"id": "问题类型", "visible": True},
            {"id": "商品编码", "visible": True},
            {"id": "店铺款式编码", "visible": True},
            {"id": "原始线上订单号", "visible": True},
            {"id": "款式编码", "visible": True},
            {"id": "产品分类", "visible": True},
            {"id": "虚拟分类", "visible": True},
            {"id": "商品简称", "visible": True},
            {"id": "颜色规格", "visible": True},
            {"id": "线上颜色规格", "visible": True},
            {"id": "品牌", "visible": True},
            {"id": "供应商", "visible": True},
            {"id": "供应商款号", "visible": True},
            {"id": "供应商商品编码", "visible": True},
            {"id": "商品标签", "visible": True},
            {"id": "组合装实体编码", "visible": True},
            {"id": "基本售价", "visible": True},
            {"id": "成本价", "visible": True},
            {"id": "cost_type", "visible": True},
            {"id": "销售数量", "visible": True},
            {"id": "赠品数量", "visible": True},
            {"id": "sent_qty", "visible": True},
            {"id": "实发金额", "visible": True},
            {"id": "销售金额", "visible": True},
            {"id": "销售成本", "visible": True},
            {"id": "实发成本", "visible": True},
            {"id": "销售毛利", "visible": True},
            {"id": "销售毛利率", "visible": True},
            {"id": "已付金额", "visible": True},
            {"id": "应付金额", "visible": True},
            {"id": "售价", "visible": True},
            {"id": "基本金额", "visible": True},
            {"id": "当期退货数量", "visible": True},
            {"id": "当期实退数量", "visible": True},
            {"id": "当期退货金额", "visible": True},
            {"id": "当期退货成本", "visible": True},
            {"id": "当期实退成本", "visible": True},
            {"id": "当期实退金额", "visible": True},
            {"id": "运费收入", "visible": True},
            {"id": "运费收入分摊", "visible": True},
            {"id": "运费支出", "visible": True},
            {"id": "运费支出分摊", "visible": True},
            {"id": "优惠金额", "visible": True},
            {"id": "订单重量", "visible": True},
            {"id": "订单商品重量", "visible": True},
            {"id": "shop_site", "visible": True},
            {"id": "订单来源", "visible": True},
            {"id": "便签", "visible": True},
            {"id": "其它价格1", "visible": True},
            {"id": "其它价格2", "visible": True},
            {"id": "其它价格3", "visible": True},
            {"id": "其它价格4", "visible": True},
            {"id": "其它价格5", "visible": True},
            {"id": "其它属性1", "visible": True},
            {"id": "其它属性2", "visible": True},
            {"id": "其它属性3", "visible": True},
            {"id": "其它属性4", "visible": True},
            {"id": "其它属性5", "visible": True},
            {"id": "其它属性6", "visible": True},
            {"id": "其它属性7", "visible": True},
            {"id": "其它属性8", "visible": True},
            {"id": "其它属性9", "visible": True},
            {"id": "其它属性10", "visible": True},
            {"id": "供销商", "visible": True},
            {"id": "shop_id", "visible": True},
            {"id": "线上商品名", "visible": True},
            {"id": "业务员", "visible": True},
            {"id": "币种", "visible": True},
            {"id": "境外运费支出分摊", "visible": True},
            {"id": "境外收入总计分摊", "visible": True},
            {"id": "境外支出总计分摊", "visible": True},
            {"id": "组合装商品编码", "visible": True},
            {"id": "商品状态", "visible": True},
            {"id": "退款状态", "visible": True},
            {"id": "线上子订单编号", "visible": True},
            {"id": "出仓单号", "visible": True},
            {"id": "promotion_names", "visible": True},
            {"id": "商品资料设置重量", "visible": True},
            {"id": "rate_cny", "visible": True},
            {"id": "seller_flag", "visible": True},
            {"id": "buyer_receive_refund", "visible": True},
        ]

    def build_request_data(self, shop_id: str, start_date: str, end_date: str, page: str = "1") -> dict:
        """构建完整的请求数据"""
        search = [
            {"k": "nolabels", "v": "黑色标,特殊单,无效订单", "c": "=", "t": ""},
            {"k": "cost_type", "v": "1", "c": "@=", "t": ""},
            {"k": "A.status", "v": "WAITCONFIRM,WAITDELIVER,DELIVERING,SENT,QUESTION,WAITOUTERSENT,CANCELLED",
             "c": "@=", "t": ""},
            {"k": "E.aftertype", "v": "普通退货,仅退款,拒收退货,换货,补发,投诉,其它,现场退货", "c": "@=", "t": ""},
            {"k": "C.afterstatus", "v": "CONFIRMED", "c": "@=", "t": ""},
            {"k": "A.shop_id", "v": shop_id, "c": "@=", "t": ""},
            {"k": "combinesku_type", "v": 2, "c": "@=", "t": ""},
            {"k": "combinesku", "v": 1, "c": "@=", "t": ""},
            {"k": "is_currency", "v": 0, "c": "@=", "t": ""},
            {"k": "C.item_pay_date", "v": start_date, "c": ">=", "t": "date"},
            {"k": "export_date_begin", "v": start_date, "c": ">="},
            {"k": "C.item_pay_date", "v": end_date, "c": "<", "t": "date"},
            {"k": "export_date_end", "v": end_date, "c": "<"},
            {"k": "C.confirm_date", "v": start_date, "c": ">=", "t": "date"},
            {"k": "C.confirm_date", "v": end_date, "c": "<", "t": "date"},
        ]

        # 关键修复：Args 需要包含 4 个元素，第4个是可见列配置
        callback_param = {
            "Method": "LoadDataToJSON",
            "Args": [
                page,
                None,
                json.dumps({"fld": "内部订单号", "type": "desc"}),
                json.dumps(self.get_visible_columns(), ensure_ascii=False)  # 添加可见列配置
            ],
            "CallControl": "{page}"
        }

        return {
            "__VIEWSTATE": "/wEPDwUKLTkzNTUxNDU5N2RkPQuMOHZ95enyVpPy/ZQtTNOdakU=",
            "__VIEWSTATEGENERATOR": "AB3F65CF",
            "search": json.dumps(search, ensure_ascii=False),
            "dataPageCount": "",
            "__CALLBACKID": "ACall1",
            "__CALLBACKPARAM": json.dumps(callback_param, ensure_ascii=False)
        }

    async def get_info(self):
        url = "https://bi.erp321.com/app/daas/report/subject/adsfinance/detail.aspx"
        params = {
            "r": "0.5585252346454567",
            "___skutype": "combinesku",
            "ts___": "1776743237170",
            "am___": "LoadDataToJSON"
        }
        # 获取时间范围
        start_date = self.start_date
        end_date_obj = datetime.strptime(self.end_date, '%Y-%m-%d').date()
        end_date = (end_date_obj +timedelta(days=1)).strftime("%Y-%m-%d")

        data = self.build_request_data(self.shop_id, start_date, end_date)

        # 注意：这里可能需要添加 headers 和 cookies
        json_data = await self.post(url, params=params, payload=data)
        return json_data

    async def parse(self, data):
        """解析返回数据"""
        if not data or 'datas' not in data:
            return None, None

        if data['datas']:
            last_data = data['datas'][-1]
            sales_amount = data['datas'][-1]['销售金额']
            cost_of_sales = data['datas'][-1]['销售成本']
            return sales_amount, cost_of_sales

        return None, None

    async def main(self):
        result = await self.get_info()
        sales_amount, cost_of_sales = await self.parse(result)
        return {
            'sales_amount': sales_amount,
            'cost_of_sales': cost_of_sales,
            'raw_data': result
        }


async def test_multiple_shops():
    shop_id_item = {
        "嬉游记-1688": "12426494",
        "汕头俏娃-1688": "12430140",
        "嬉游记-线下": "14836828",
        "汕头俏娃-线下": "14836831",
    }
    start_date = "2026-03-01"
    end_date = "2026-03-31"

    results = {}
    for shop_name, shop_id in shop_id_item.items():
        print(f"\n正在获取 {shop_name} 的数据...")
        client = PaymentCostClient('chinese_financial_statements', shop_id, start_date, end_date)

        try:
            result = await client.main()
            results[shop_name] = result
            print(f"✅ {shop_name}: 销售金额={result['sales_amount']}, 销售成本={result['cost_of_sales']}")

        except Exception as e:
            print(f"❌ {shop_name}: 获取失败 - {e}")
            results[shop_name] = {'error': str(e)}


# if __name__ == '__main__':
#     # 测试多个店铺
#     asyncio.run(test_multiple_shops())
