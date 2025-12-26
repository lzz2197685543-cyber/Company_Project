# api/offer_api_capture.py
class OfferApiCapture:

    API_KEYWORD = "/offerSearch/queryList"

    def __init__(self, page):
        self.page = page

    async def capture_once(self):
        print("⏳ 等待点击【查询】并捕获 offerSearch 请求...")

        async with self.page.expect_request(
            lambda r: self.API_KEYWORD in r.url,
            timeout=30000
        ) as req_info:

            # ⭐ 关键：点击页面“查询”按钮（真实前端行为）
            await self.page.click("button:has-text('查询')")

        req = await req_info.value

        print("✅ 捕获到前端真实请求")

        return {
            "url": req.url,
            "method": req.method,
            "headers": dict(req.headers),
            "payload": req.post_data_json,
        }
