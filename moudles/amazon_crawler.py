import asyncio
from core.base_client import LingXingBaseClient


class AmazonCrawler(LingXingBaseClient):
    URL = 'https://gw.lingxingerp.com/sc/restocking-center/amazon/analysis/list'

    async def fetch(self):
        json_data = {
            'offset': 20,
            'length': 200,
            'searchField': 'asin',
            'searchValue': '',
            'star': '0',
            'fulfillmentChannelType': '',
            'searchDate': 'sugDatePurchase',
            'restockStatus': [
                0,
            ],
            'tagIdList': [],
            'sortField': 'outStockDate',
            'sortType': 'asc',
            'sortKey': '',
            'dataType': 1,
            'analysisMode': 1,
            'cidList': [],
            'bidList': [],
            'listingStatusList': [],
            'storeIdList': [
                294,
                293,
                2373,
                2557,
                3422,
                3425,
                3409,
                3412,
                3410,
                297,
                1480,
            ],
            'principalUidList': [],
            'seniorSearchList': [],
            'rangeSelectList': [],
            'req_time_sequence': '/sc/restocking-center/amazon/analysis/list$$11',
        }

        data=await self.post(self.URL, json_data)
        return data

    async def _parse(self,data):
        print(data)

    async def fetch_all_pages(self):
        """"""
        all_items=[]
        page=1

async def main():
    scraper = AmazonCrawler('Amazon_Stock_Watcher')
    data=await scraper.fetch()
    # print(data)

if __name__ == '__main__':
    asyncio.run(main())