from core.base_client import TemuBaseClient
import asyncio
import re

class SkuException(TemuBaseClient):
    URL='https://agentseller.temu.com/api/kiana/mms/robin/queryFullyOtherMessage'

    async def fetch(self,goodsIdSkuIdPairList):
        payload = {
            'goodsIdSkuIdPairList':goodsIdSkuIdPairList
        }

        json_data = await self.post(self.URL, payload)
        return self._parse(json_data)

    def _parse(self, json_data):
        rows=[]
        # 1️⃣ 接口层失败兜底
        if not isinstance(json_data, dict):
            self.logger.warning(f"sku_site_status 返回非 dict: {json_data}")

        result = json_data.get("result")
        if not result:
            self.logger.warning(f"sku_site_status result 为空: {json_data}")
            return rows

        if result['fullyBindSiteFailVO'] is None:
            self.logger.warning(f"sku_site_status fullyBindSiteFailVO 为空: 没有异常数据")
            return []

        for i in result['fullyBindSiteFailVO']['goodsSkuBindSiteFailVOList']:
            goods_sku_id = i['goodsSkuId']

            for j in i['goodsSkuBindSiteFailInfoVOList']:
                site = j['siteName']

                raw_desc = j['failResultVOList'][0]['checkDesc']
                reasons = self.extract_reasons(raw_desc)

                rows.append({'skuId':goods_sku_id,'站点':site,'异常原因':','.join(reasons)})
        return rows

    def extract_reasons(self,raw):
        if not raw:
            return []

        # 1️⃣ 去 HTML 标签
        text = re.sub(r'<[^>]+>', '', raw)

        # 2️⃣ 去【详情】类
        text = re.sub(r'【.*?】', '', text)

        # 3️⃣ 拆分
        parts = re.split(r'[；;\n/]', text)

        reasons = set()
        for p in parts:
            p = p.strip()

            # 4️⃣ 过滤垃圾
            if not p:
                continue
            if p.lower() == 'none':
                continue
            if p.isdigit():
                continue
            if p == '儿童玩具':
                continue

            # # 5️⃣ 只保留“像原因的内容”
            # if any(k in p for k in (
            #         '缺失', '不合规', '禁止', '不支持', '资质', '检测','无线',"限制"
            # )):
            reasons.add(p)

        return list(reasons)



async def main():
    s = SkuException("106-Temu全托管",'temu_site')
    await s.fetch(11)


# if __name__ == "__main__":
#     asyncio.run(main())











