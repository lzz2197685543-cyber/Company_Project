from bs4 import BeautifulSoup
from utils.headermanager import HeaderManager
from utils.requestmanager import RequestManager
from utils.logger import get_logger
from pathlib import Path
import json
import re

CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "data"


class AmazonCoupon:
    def __init__(self, url,job):
        self.request_manager = RequestManager(
            job=job,
            header_manager=HeaderManager(),
            use_proxy=True,  # 启用代理
            max_retries=3
        )
        self.url = url

        self.logger = get_logger(job)

    def get_amazon_coupon(self):
        self.logger.info('-----------开始爬取优惠券信息-----------')
        """获取亚马逊优惠券"""
        try:
            # 先测试代理池
            # print("测试代理池可用性...")
            # self.request_manager.proxy_pool.test_all()

            self.logger.info(f"开始抓取: {self.url}")

            # 发送请求
            response = self.request_manager.get(
                self.url,
                use_amazon_headers=True,
                timeout=15
            )

            print(response.text[2000:3000])

            if not response:
                self.logger.info("❌ 无法获取页面")
                return None

            # 解析页面
            soup = BeautifulSoup(response.text, 'html.parser')

            # 获取标题
            title = soup.find('span', {'id': 'productTitle'})
            if title:
                self.logger.info(f"标题: {title.text.strip()}")

            # 获取评分
            rating=soup.select_one('span #acrPopover .a-size-small')
            if rating:
                rating_text=rating.get_text(strip=True)
                print('评分是：',rating_text)

            # 评分数
            reviews=soup.select_one('span #acrCustomerReviewText')

            if reviews:
                reviews_text=reviews.get_text(strip=True).replace('(','').replace(')','')
                print('评分人数:', reviews_text)


            # 查找优惠券
            coupon = soup.find('span', {'class': 'a-color-success couponLabelText'})
            if coupon:
                coupon_text = coupon.get_text(strip=True)
                self.logger.info(f"优惠券: {coupon_text}")

                # 提取金额
                import re
                match = re.search(r'([\$￥€]?\d+\.?\d*%?)', coupon_text)
                if match:
                    return match.group(1)
                return coupon_text
            else:
                self.logger.info("没有优惠券")
                return ''

        except Exception as e:
            self.logger.error(f"❌ 错误: {e}")
            return None

    def parse_amazon_product(self):
        response = self.request_manager.get(
            self.url,
            use_amazon_headers=True,
            timeout=15
        )


        if not response:
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        result = {
            "title": None,
            "rating": None,
            "review_count": None,
            "coupon": None
        }

        # -------------------------
        # 标题
        # -------------------------
        title = soup.select_one("#productTitle")

        if not title:
            title = soup.select_one("#title")

        if title:
            result["title"] = title.get_text(strip=True)

        # -------------------------
        # 评分
        # -------------------------
        rating = soup.select_one("#acrPopover span.a-size-base")

        if not rating:
            rating = soup.select_one('[data-hook="average-star-rating"] span')

        if not rating:
            rating = soup.select_one('.a-icon-star span')

        if rating:
            result["rating"] = rating.get_text(strip=True)

        # -------------------------
        # 评分人数
        # -------------------------
        reviews = soup.select_one("#acrCustomerReviewText")

        if not reviews:
            reviews = soup.select_one('[data-hook="total-review-count"]')

        if reviews:
            review_text = reviews.get_text(strip=True)

            match = re.search(r"[\d,]+", review_text)

            if match:
                result["review_count"] = match.group(0).replace(",", "")

        # -------------------------
        # coupon
        # -------------------------
        coupon = soup.select_one(".couponLabelText")

        if coupon:
            coupon_text = coupon.get_text(strip=True)

            match = re.search(r'([\$€£]?\d+%?|[\$€£]?\d+\.\d+)', coupon_text)

            if match:
                result["coupon"] = match.group(1)
            else:
                result["coupon"] = coupon_text

        rating_text = soup.select_one('[data-hook="rating-out-of-text"]')



        # -------------------------
        # JSON结构兜底
        # -------------------------
        if not result["rating"] or not result["review_count"]:

            scripts = soup.find_all("script", type="application/ld+json")

            for script in scripts:

                try:
                    if not script.string:
                        continue

                    data = json.loads(script.string)

                    if isinstance(data, dict) and "aggregateRating" in data:

                        rating_data = data["aggregateRating"]

                        if not result["rating"]:
                            result["rating"] = rating_data.get("ratingValue")

                        if not result["review_count"]:
                            result["review_count"] = rating_data.get("reviewCount")

                        break

                except:
                    pass

        return result

    def close(self):
        """关闭"""
        self.request_manager.close()


def main():
    # df=pd.read_excel(f'{CONFIG_DIR}/14.xlsx')
    # for url in list(df['产品链接']):

    # 测试
    url = 'https://www.amazon.com/dp/B0FX3Q1JS4?th=1'

    # 创建爬虫
    crawler = AmazonCoupon(url,'amazon_goods_monitor')

    # 抓取
    data = crawler.parse_amazon_product()

    result=crawler.get_amazon_coupon()


    # 结果
    print(f"\n{'=' * 50}")
    print(f"结果: {result if result else '无优惠券'}")

    # 关闭
    crawler.close()

#
if __name__ == '__main__':
    main()