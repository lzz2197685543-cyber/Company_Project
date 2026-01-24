from core.base_client import TemuBaseClient
import asyncio


class SkcFetcher(TemuBaseClient):
    URL = "https://agentseller.temu.com/api/kiana/mms/robin/searchForChainSupplier"

    async def fetch(self):


        payload = {
            "pageSize": 100,
            "pageNum": 1,
            "secondarySelectStatusList": [12],
            "supplierTodoTypeList": [6],
        }

        data = await self.post(self.URL, payload)
        return self._parse(data)

    def _parse(self, json_data):
        parsed_items = self._parse_raw_items(json_data)

        violate_rows = self.build_violate_rows(parsed_items)
        goods_sku_pairs = self.build_goods_sku_pair_list(parsed_items)

        return {
            "violate_rows": violate_rows,
            "goodsIdSkuIdPairList": goods_sku_pairs
        }

    def _parse_raw_items(self, json_data):
        items = []

        for item in json_data.get("result", {}).get("dataList", []):
            try:
                skc = item["skcList"][0]

                items.append({
                    "goodsId": item["goodsId"],
                    "skcId": skc["skcId"],
                    "skuList": [
                        {
                            "skuId": sku["goodsSkuId"],
                            "skuCode": sku.get("extCode")
                        }
                        for sku in skc.get("skuList", [])
                    ],
                    "punishList": [
                        {
                            "site": p.get("site"),
                            "reason": p.get("reason")
                        }
                        for p in item.get("allPunishInfoList", [])
                    ]
                })

            except (KeyError, IndexError, TypeError):
                continue

        return items

    def build_violate_rows(self, parsed_items):
        rows = []

        for item in parsed_items:
            for sku in item["skuList"]:
                for punish in item["punishList"]:
                    rows.append({
                        "SKC": item["skcId"],
                        "SKU": sku["skuCode"],
                        "skuId": sku["skuId"],
                        "站点": punish["site"],
                        "违规原因": punish["reason"],
                    })

        return rows

    def build_goods_sku_pair_list(self, parsed_items):
        return [
            {
                "goodsId": item["goodsId"],
                "skuIdList": [sku["skuId"] for sku in item["skuList"]]
            }
            for item in parsed_items
        ]


# async def main():
#     s = SkcFetcher("106-Temu全托管",'temu_site')
#     skcs = await s.fetch()
#
#
# if __name__ == "__main__":
#     asyncio.run(main())
