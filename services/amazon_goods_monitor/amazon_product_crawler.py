import asyncio

from services.amazon_goods_monitor.get_amazon_price  import AmazonMonitorPrice
from services.amazon_goods_monitor.get_amazon_popularity import AmazonMonitorPopularity
from services.amazon_goods_monitor.get_amazon_coupon import AmazonCoupon
from datetime import date

class AmazonProductCrawler:

    def __init__(self, job):
        self.job = job

        self.price_monitor = AmazonMonitorPrice(job)
        self.popularity_monitor = AmazonMonitorPopularity(job)

    async def crawl_one(self, product_name, url):

        asin=url.split('/')[-1]
        if "?" in asin:
            asin=asin.split('?')[0]
        else:
            asin=asin

        product = {
            "asin": asin,
            "product_name":product_name,
            'url':url,
            'date':date.today(),
            'title_changed':'否'
        }

        # ---------------- 价格数据 ----------------
        price_data = await self.price_monitor.fetch(asin)

        print(price_data)

        items = price_data.get("data", {}).get("items", []) if price_data else []

        if items:
            for item in items:
                if asin == item['asin']:
                    product['title'] = item.get('title')
                    product["price"] = item.get("price")
                    product["sales"] = item.get("amzUnit") or item.get("totalUnits")

                    product["bsr_rank"] = item.get("bsrRank")
                    product["rating"] = item.get("rating")
                    product["reviews"] = item.get("reviews")
                    product["img_url"] = item.get("bigImageUrl")

                    # 子类排名（防止 subcategories 为空）
                    subcategories = item.get("subcategories", [])
                    if subcategories:
                        product["sub_rank"] = subcategories[0].get("rank")
                    else:
                        product["sub_rank"] = None


        # ---------------- 流量数据 ----------------
        popularity_data = await self.popularity_monitor.fetch(asin)

        if popularity_data:
            pager = popularity_data.get("data", {}).get("pager")

            if pager and pager.get("items"):
                items = pager["items"]

                for item in items:
                    if asin == item['asin']:
                        # 全部流量词
                        product["all_keywords"] = item.get("keywords", 0)
                        # 自然搜索词
                        product["natural_keywords"] = item.get("counter", {}).get("NATURAL_SEARCHING", 0)
                        # 广告流量词
                        product["ads_keywords"] = item.get("counter", {}).get("ADS", 0)+item['counter'].get('SPONSOR_VIDEO',0)+item['counter'].get('HIGHLY_RATED',0)
                        # 搜索关键词
                        product["recommend_keywords"] = item.get("counter", {}).get("AMAZON_CHOICE", 0)

            else:
                product["all_keywords"] = 0
                product["natural_keywords"] = 0
                product["ads_keywords"] = 0
                product["recommend_keywords"] = 0

        # ---------------- coupon ----------------
        coupon_crawler = AmazonCoupon(url, self.job)
        coupon = coupon_crawler.get_amazon_coupon()
        coupon_crawler.close()

        product["coupon"] = coupon


        return product

    async def crawl_batch(self, asin_url_list):

        result = []

        for item in asin_url_list:
            product_name=item['产品名']
            url=item['产品链接']

            product = await self.crawl_one(product_name, url)

            result.append(product)

        return result


# asin_url_list = [
#     ("配对花-Learning Resources", "https://www.amazon.com/dp/B0DSGL45JX?th=1"),
#     ("弹珠平衡-Zamtzax", "https://www.amazon.com/dp/B0FQNZNP5P?th=1")
# ]
#
# async def main():
#
#     crawler = AmazonProductCrawler("amazon_goods_monitor")
#
#     product_list = await crawler.crawl_batch(asin_url_list)
#
#     print(product_list)
#
# if __name__ == '__main__':
#     asyncio.run(main())