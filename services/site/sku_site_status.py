from core.base_client import TemuBaseClient
import asyncio


class SkuSiteStatusFetcher(TemuBaseClient):
    URL = "https://agentseller.temu.com/darwin-mms/api/kiana/southstar/qtg/bindsite/querySkcBindSiteStatus"

    async def fetch(self, skc_id: int) -> list[dict]:
        payload = {"productSkcId": skc_id}
        data = await self.post(self.URL, payload)
        return self._parse(data)

    def _parse(self, json_data):
        rows = []

        # 1️⃣ 接口层失败兜底
        if not isinstance(json_data, dict):
            self.logger.warning(f"sku_site_status 返回非 dict: {json_data}")
            return rows

        if not json_data.get("success", True):
            self.logger.warning(
                f"sku_site_status 接口失败 "
                f"errorCode={json_data.get('errorCode')} "
                f"errorMsg={json_data.get('errorMsg')}"
            )
            return rows

        result = json_data.get("result")
        if not result:
            self.logger.warning(f"sku_site_status result 为空: {json_data}")
            return rows

        # 2️⃣ 正常解析
        for sku in result.get("skuBindSiteStatusList", []):
            sku_code = sku.get("extCode")

            for site in sku.get("bindSiteStatusList", []):
                rows.append({
                    "SKU": sku_code,
                    "站点": site.get("siteName"),
                    "加站状态": "已加站" if site.get("bindStatus") == 1 else "未加站",
                })

        return rows

