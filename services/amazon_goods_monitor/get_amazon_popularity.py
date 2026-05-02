import asyncio
from core.base_client import SellerSpriteClient

class AmazonMonitorPopularity(SellerSpriteClient):
    def __init__(self, job):
        super().__init__(job)
        self.url = 'https://www.sellersprite.com/v3/api/relation/ta/source'

    async def fetch(self, asins):
        self.logger.info('-----------开始爬取商品流量信息-----------')
        try:
            params = {
                'keywordOrAsin': asins,
                'market': 'COM',
                'pageNo': '1',
                'pageSize': '20',
                'order': '1',
                'desc': 'true',
                'month': '',
            }
            return await self.get(self.url, params)
            # return await self.get(self.url, params),asins
        except Exception as e:
            self.logger.error(f"请求失败: {e}")
            return None

    async def parse(self, res_data,asins):
        try:
            if res_data and 'data' in res_data and 'pager' in res_data['data']:
                for i in res_data['data']['pager']['items']:
                    print(i)
                    if asins == i['asin']:
                        # 全部流量词
                        all_keywords=i['keywords']
                        # 自然搜索词
                        search_key=i['counter'].get('NATURAL_SEARCHING',0)

                        # 广告流量词
                        advertisement_key=i['counter'].get('ADS',0)+i['counter'].get('SPONSOR_VIDEO',0)+i['counter'].get('HIGHLY_RATED',0)+i['counter'].get('SPONSOR_BRAND',0)

                        # 搜索关键词
                        keywords=i['counter'].get('AMAZON_CHOICE',0)

                        item_list = {
                            '全部流量词': all_keywords,
                            '自然搜索词': search_key,
                            '广告流量词': advertisement_key,
                            '搜索关键词': keywords
                        }

                        print(item_list)
            else:
                self.logger.info('get_amazon_popularity返回数据格式错误')
        except Exception as e:
            self.logger.error(f"get_amazon_popularity解析失败: {e}")
            return None, None

async def main():
    monitor = AmazonMonitorPopularity('amazon_goods_monitor')
    data ,asins= await monitor.fetch('B0FVMFGM5W')
    if data:
        await monitor.parse(data,asins)

    else:
        print("请求失败")

# if __name__ == '__main__':
#     asyncio.run(main())