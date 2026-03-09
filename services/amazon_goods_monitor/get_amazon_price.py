import asyncio
from core.base_client import SellerSpriteClient


class AmazonMonitorPrice(SellerSpriteClient):
    def __init__(self, job):
        super().__init__(job)
        self.url = 'https://www.sellersprite.com/v3/api/competing-lookup'

    async def fetch(self, asins):
        self.logger.info('-----------开始爬取商品信息-----------')
        try:
            json_data = {
                'market': 'US',
                'monthName': 'bsr_sales_nearly',
                'asins': [asins],
                'page': 1,
                'nodeIdPaths': [],
                'symbolFlag': False,
                'size': 60,
                'order': {'field': 'amz_unit', 'desc': True},
                'lowPrice': 'N'
            }
            return await self.post(self.url, json_data)
            # return await self.post(self.url, json_data),asins
        except Exception as e:
            self.logger.error(f"请求失败: {e}")
            return None

    async def parse(self, res_data,asins):
        try:
            if res_data and 'data' in res_data and 'items' in res_data['data']:
                for item in res_data['data']['items']:
                    if asins==item['asin']:
                        title=item['title']
                        # 价格
                        price = item['price']

                        # 销量，有子体销量就取子体销量，否则取父销量
                        if item["amzUnit"]:
                            total_sales = item["amzUnit"]
                        else:
                            total_sales = item['totalUnits']

                        img_url = item['bigImageUrl']

                        # 大类排名
                        bsr_rank = item['bsrRank']

                        # 小类排名
                        rank = item['subcategories'][0]['rank']

                        # 评分
                        rating = item['rating']

                        # 评分数
                        reviews = item['reviews']

                        item_list = {
                            "标题":title,
                            '价格': price,
                            '销量': total_sales,
                            '图片URL': img_url,
                            '大类排名': bsr_rank,
                            '小类排名': rank,
                            '评分': rating,
                            '评论数': reviews
                        }
                        print(item_list)

            else:
                self.logger.info("get_amazon_price返回数据格式错误")
                return None, None
        except Exception as e:
            self.logger.error(f"get_amazon_price解析失败: {e}")
            return None, None

#
async def main():
    monitor = AmazonMonitorPrice('amazon_goods_monitor')
    data,asins = await monitor.fetch('B0FVMFGM5W')
    if data:
        await monitor.parse(data,asins)

    else:
        print("请求失败")

# if __name__ == '__main__':
#     asyncio.run(main())
