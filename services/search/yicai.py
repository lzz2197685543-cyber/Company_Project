import time
import asyncio
from core.base_client import YiCaiClient
from services.search.yicqai_search_playwright import YiCaiImageSearch
from services.search.xiaozhuxiong import XiaozhuxiongSearch

class YiCaiSearch(YiCaiClient):
    URL_TEMPLATE = 'https://www.essabuy.com/toys/search-by-image?fid={}&req_id=176835391066824968'

    def __init__(self):
        super().__init__()
        self.fid = None
        self.URL = None

        self.search = XiaozhuxiongSearch()


    async def get_fid(self, image_path: str):
        """通过 Playwright 上传图片获取 fid"""
        searcher = YiCaiImageSearch(headless=True)
        fid = await searcher.get_fid(image_path)
        self.fid = fid
        self.URL = self.URL_TEMPLATE.format(fid)

        print('拿到fid：',fid)
        return fid

    async def fetch(self, image_path: str):
        """主流程：先拿 fid，再请求搜索接口"""
        # 1️⃣ 拿 fid
        await self.get_fid(image_path)

        # 2️⃣ 请求搜索接口
        json_data = await self.get(self.URL)
        upload_img_url = self.search.get_img_url(image_path)
        items = await self._parse(json_data,upload_img_url)
        return items

    async def _parse(self, json_data,upload_img_url):
        items = []
        for i in json_data['models'][:10]:
            detail_url = f'https://www.essabuy.com/{i["spuId"]}/{i["skuNo"]}/{i["enSpuName"]}.html'
            detail_json = await self.get(detail_url)
            detail_data = detail_json['detail']['model']['brandDetailVO']['portalContactBrandVO']

            item_dict = {
                "平台": '宜采',
                "搜图图片": upload_img_url,
                "商品名称": i['spuName'],
                "商品图片链接": 'https:' + i['skuList'][0]['imageUrl']['originalImg'],
                "价格": i['skuList'][0]['price'],
                "供应商": i['skuList'][0]['supplierName'],
                "联系人": detail_data.get('name'),
                "手机号": detail_data.get('mobile'),
                "QQ": detail_data.get('email'),
                "地址": detail_data.get('address'),
                '爬取数据时间': int(time.time() * 1000),
            }
            print(item_dict)
            items.append(item_dict)
        return items

if __name__ == '__main__':
    image_path = r"D:\sd14\Factory_sourcing\data\img\basketball.png"
    y = YiCaiSearch()
    items = asyncio.run(y.fetch(image_path))
    print(f"总共抓取 {len(items)} 条数据")
