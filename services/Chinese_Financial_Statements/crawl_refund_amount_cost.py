import asyncio
import urllib.parse
import json
from core.base_client import BaseClient
from datetime import datetime, timedelta
import requests
import re


class RefundAmountClient(BaseClient):
    def __init__(self, job, shop_id, start_date:str, end_date:str):
        super().__init__(job)
        self.shop_id = shop_id
        self.start_date = start_date
        self.end_date = end_date

    def build_search_params(self, shop_id, start_date, end_date):
        """构建 search 参数"""
        search = [
    {"k":"cost_type","v":"1","c":"@=","t":""},
    {"k": "A.shop_id", "v": f"{shop_id}", "c": "@=", "t": ""},
    {"k": "coalesce(A.status,'')", "v": "CONFIRMED", "c": "@=", "t": ""},
    {"k": "A.status", "v": "CONFIRMED", "c": "@=", "t": ""},
    {"k": "A.type", "v": "普通退货,仅退款,拒收退货,换货,补发,投诉,其它,现场退货", "c": "@=", "t": ""},
    {"k": "A.confirm_date", "v": f"{start_date}", "c": ">=", "t": "date"},
    {"k": "A.confirm_date", "v": f"{end_date}", "c": "<", "t": "date"},
    {"k": "coalesce(A.order_status,'')", "v": "ALL", "c": "@=", "t": ""},
    {"k": "A.order_status", "v": "ALL", "c": "@=", "t": ""},
    {"k": "combinesku", "v": 1, "c": "@=", "t": ""},
    {"k": "is_currency", "v": 0, "c": "@=", "t": ""}
]
        return json.dumps(search, ensure_ascii=False)

    def build_callback_param(self):
        """构建 __CALLBACKPARAM 参数"""
        callback_param = {
            "Method": "LoadDataToJSON",
            "Args": ["1", None, '{"fld":"售后单号","type":"desc"}'],
            "CallControl": "{page}"
        }
        return json.dumps(callback_param, ensure_ascii=False)

    async def get_VIEWSTATE(self):
        url = "https://bi.erp321.com/app/daas/report/subject/adsaftersale/detailafter.aspx"

        text = await self.get(url)

        vie = re.findall(r'<input type="hidden" name="__VIEWSTATE" id="__VIEWSTATE" value="(.*?)" />', text)[0]

        return vie

    async def get_info(self):
        url = 'https://bi.erp321.com/app/daas/report/subject/adsaftersale/detailafter.aspx'

        # 固定参数
        fixed_params = {
            'r': '0.49231175430418683',
            '___skutype': 'combinesku',
            'ts___': '1776755381761',
            'am___': 'LoadDataToJSON',
        }

        # 获取时间范围
        start_date = self.start_date
        end_date_obj = datetime.strptime(self.end_date, '%Y-%m-%d').date()
        end_date = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")


        # 构建动态参数
        search = self.build_search_params(self.shop_id, start_date, end_date)
        callback_param = self.build_callback_param()

        vie=await self.get_VIEWSTATE()
        print(vie)

        # 方法1：直接构建表单数据（推荐，框架会自动处理URL编码）
        data = {
            '__VIEWSTATE': f'{vie}',
            '__VIEWSTATEGENERATOR': '67BBF780',
            'search': search,
            'dataPageCount': '',
            '__CALLBACKID': 'ACall1',
            '__CALLBACKPARAM': callback_param
        }

        # 方法2：如果需要手动编码（某些框架可能需要）
        # encoded_data = urllib.parse.urlencode(data, encoding='utf-8', doseq=False)

        self.logger.info(f"请求参数: shop_id={self.shop_id}, start={start_date}, end={end_date}")

        # 发送请求（BaseClient 的 post 方法应该会自动处理表单数据）
        json_data = await self.post(url, payload=data, params=fixed_params)
        return json_data

    async def parse(self, data):
        """解析返回数据"""
        if not data or 'datas' not in data:
            return None, None

        if data['datas']:
            last_data = data['datas'][-1]
            refund_amount = last_data.get('退货金额', '0')
            refund_cost = last_data.get('退货成本金额', '0')
            return refund_amount, refund_cost

        return None, None

    async def main(self):
        result = await self.get_info()
        refund_amount, refund_cost = await self.parse(result)
        return {
            'refund_amount': refund_amount,
            'refund_cost': refund_cost,
            'raw_data': result
        }


async def test_multiple_shops():
    """测试多个店铺"""
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
        client = RefundAmountClient('chinese_financial_statements', shop_id, start_date, end_date)

        try:
            result = await client.main()
            results[shop_name] = result
            print(f"✅ {shop_name}: 退货金额={result['refund_amount']}, 退货成本={result['refund_cost']}")
        except Exception as e:
            print(f"❌ {shop_name}: 获取失败 - {e}")
            results[shop_name] = {'error': str(e)}

    return results


# if __name__ == '__main__':
#     # 测试多个店铺
#     asyncio.run(test_multiple_shops())