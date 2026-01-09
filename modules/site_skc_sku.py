import requests
from pathlib import Path
import csv
from datetime import datetime


class Temu_skc_sku:
    def __init__(self, shop_name):
        self.shop_name = 'temu_103'
        self.cookies = {
            'api_uid': 'Ct3z+mflAq8gZwBML1BSAg==',
            '_nano_fp': 'XpmYXq9jXpEJXqPql9_GOCN8173MDe_o_dSda51Y',
            'hfsc': 'L3yOe4k26Djw1Z/OeA==',
            'dilx': '_S4dl_2I1TVdDNgFvIbO7',
            'njrpl': 'O5yJ6R0zP0rd8QarAQaUwKgZX6n3OapF',
            '_bee': 'O5yJ6R0zP0rd8QarAQaUwKgZX6n3OapF',
            'mallid': '728287620794',
            'gmp_temu_token': 'I60jWbUi2z7XTOhtdy69hh25JP9q4/9tArtVBee+adHjEI7FelZDz2RKwpI6Q76pC8+Q6WuB/97PCC1vfJVRQ/3O/8cav0jTjVOZin4n1Pcjpy1w2PTF2SV15dG3DpW9rpIsqX7eYfRmBJCPA8+U0xN4YUWzqKT22P/+9T2corc',
            'webp': '1',
            'timezone': 'Asia%2FShanghai',
            'img_sup': 'avif%2Cwebp',
            'region': '0',
            'seller_temp': 'N_eyJ0IjoieUI2dkFkcENXWERBdFJ3VlIyMXRkamxobmM3TVUvQk5Yd2pJOFZsMEt0d09NSFNyRUlVV0tZaDN2QVZ1a0xDMCIsInYiOjEsInMiOjEwMDAxLCJ1Ijo5MzgyMjA3MjU3NzkyfQ==',
        }

        self.headers = {
            'mallid': '728287620794',
            'referer': 'https://agentseller.temu.com/newon/product-select',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.url = 'https://agentseller.temu.com/api/kiana/mms/robin/searchForChainSupplier'

    def get_info(self):
        json_data = {
            'pageSize': 10,
            'pageNum': 1,
            'secondarySelectStatusList': [
                12,
            ],
            'supplierTodoTypeList': [
                6,
            ],
        }

        response = requests.post(url=self.url, cookies=self.cookies, headers=self.headers, json=json_data)
        print(response.text[:200])
        return response.json()

    def parse_data(self, json_data):
        # 存储SKC-SKU关系
        skc_sku_data = {
            'SKC': [],
            'SKU': []
        }

        try:
            datas = json_data['result']['dataList']
            if not isinstance(datas, list):
                return skc_sku_data  # 返回空结构
        except (KeyError, TypeError):
            return skc_sku_data  # 返回空结构

        # 现在可以安全地处理datas
        for i in datas:
            try:
                skc = i['skcList'][0]['skcId']
                for j in i['skcList'][0]['skuList']:
                    skc_sku_data['SKC'].append(skc)
                    sku = j['extCode']
                    skc_sku_data['SKU'].append(sku)
            except (KeyError, IndexError, TypeError):
                # 跳过当前处理异常的数据项
                continue

        return skc_sku_data  # 最后返回结果

    def save_items(self, data):
        # 数据为空直接返回
        if not data or not data.get('SKC') or not data.get('SKU'):
            return

        out_dir = Path(__file__).resolve().parent.parent / "data" / "site"
        out_dir.mkdir(parents=True, exist_ok=True)

        fname = out_dir / f"{self.shop_name}_site_skc_sku_{datetime.now():%Y%m%d}.csv"
        exists = fname.exists()

        with open(fname, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=["SKC", "SKU"])

            if not exists:
                writer.writeheader()

            for skc, sku in zip(data['SKC'], data['SKU']):
                writer.writerow({
                    "SKC": skc,
                    "SKU": sku
                })


if __name__ == '__main__':
    temu = Temu_skc_sku("temu_103")
    json_data = temu.get_info()
    parsed_data = temu.parse_data(json_data)
    temu.save_items(parsed_data)
