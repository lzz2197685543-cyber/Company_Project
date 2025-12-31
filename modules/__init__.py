import requests
# 505
cookies = {
    'SPC_CDS': 'fa25179b-7844-4550-9af8-5d93bb7f87d2',
    '_gid': 'GA1.2.2112190520.1763694364',
    '_ga': 'GA1.1.1589911288.1763694364',
    '_ga_N181PS3K9C': 'GS2.1.s1766129528$o5$g1$t1766129549$j39$l0$h426184590',
    'VERIFY_PASSWORD_TOKEN': 'd7b30057-363a-4148-807f-14d84fa6613a',
    'x_region': 'CN',
    'biz_type': 'SCS',
    'lang_id': 'zhCN',
    'language': 'en',
    'userEmail': 'work221028@163.com',
    'srmid': '31230032c04645514e1a51311b258566',
    'csrf_token': '2c75e4e84acf737031b65beb47cba89d191b661c3818ee32a27d0f771da06722',
}

headers = {

    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',

    'x-region': 'CN',
}

params = {
    'csrf_token': '2c75e4e84acf737031b65beb47cba89d191b661c3818ee32a27d0f771da06722',
}

json_data = {
    'page_no': 1,
    'page_size': 100,
    'third_status_list': [],
    'actual_paid_start_time': 1764518400,
    'actual_paid_end_time': 1766937599,
    'sort_fields': [],
    'user_wait_approve_type': 0,
}

response = requests.post(
    'https://seller.scs.shopee.cn/api/v4/statement_settlement/account/api/srm/payment_request/list',
    params=params,
    cookies=cookies,
    headers=headers,
    json=json_data,
)

import time


data_list = response.json()['data']['list']

for item in data_list:
    # print(item)
    row = {
        "付款单ID": item.get('payment_request_id', ''),
        "公司名称": item.get('seller_name', ''),
        "卖家编号": item.get('seller_id', ''),
        "付款单创建日": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(item.get('ctime'))) if item.get('ctime') else '',
        "实际付款日期": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(item.get('actual_paid_date'))) if item.get('actual_paid_date') else '',
        "总计对账金额(CNY)": item.get('payout_amount', '0.00'),
        "总计对账金额(USD)": "",  # 需要汇率转换
        "总计调整金额(CNY)": item.get('adjustment_amount', '0.00'),
        "总计调整金额(USD)": "",  # 需要汇率转换
        "退货退款(CNY)": "",
        "退货退款(USD)": "",
        "卖家返利(CNY)": item.get('adjustment_amount', '0.00'),
        "卖家返利(USD)": "",
        "总打款金额(CNY)": item.get('total_payment_amount', '0.00'),
        "总打款金额(USD)": item.get('total_payment_amount_usd', ''),
        "卖家银行类型": "企业银行" if item.get('bank_type') == 1 else "个人银行",  # 根据实际映射
        "汇率": "",  # 需要额外获取
        "状态": {1: "待审批", 2: "审批中", 3: "已付款"}.get(item.get('third_status'), "未知"),  # 示例映射
        "拒绝原因": item.get('rejected_reason', '')
    }
    # print(row)


