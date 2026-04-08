import json
import copy
import asyncio

from core.base_client import BaseClient
from storage.product_dao import ProductDAO


class AutoPublish(BaseClient):

    async def get_detial_id(self):
        """获取detail_id和goods_id，detail_id是给选择店铺，获取商品信息的参数，goods_id我们是为了追踪它的状态，最后更新数据库中商品id的状态信息"""

        data_json = {
            'claimPublishShopStatus': 'published',
            'titleType': 'multi',
            'remarkType': 'multi',
            'status': 'notPublished',
            'ownerAccountIds[0]': '187908',
            'pageNo': '1',
            'pageSize': '500',
        }

        data= await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/searchCollectBoxDetail',
            payload=data_json,
        )

        items=[]

        for i in data['detailList']:
            detail_id = i['collectBoxDetailId']
            goods_id=i['sourceItemId']
            item={goods_id:detail_id}

            items.append(item)

        return items

    async def selectShop(self, detailid,shop_ids):
        """选择店铺"""
        data_json = {
            'detailIds[0]': f'{detailid}',
        }

        for i, shop_id in enumerate(shop_ids):
            data_json[f'shopIds[{i}]'] = shop_id

        data = await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/claimToShop',
            payload=data_json,
        )
        if data['result'] == 'success':
            self.logger.info(f'{detailid}:选择店铺成功')
        else:
            self.logger.info(f'{detailid}:选择店铺失败')

    async def get_goods_info(self, detailId):
        """获取商品信息"""
        data_json = {
            'detailId': f'{detailId}',
        }

        data = await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/getCollectItemInfo',
            payload=data_json,
        )

        print('获取商品信息响应：',data)
        goods_info = data.get('siteCollectItemInfo') or data.get('shopCollectItemInfo')

        self.logger.info('获取商品信息：' + str(goods_info))
        return goods_info

    def modify_goods_info(self, goods_info):
        """修改商品信息"""
        # 深拷贝原始数据，避免修改原数据
        modified_info = copy.deepcopy(goods_info)

        # 1. 修改 outerPackage 相关字段
        modified_info['outerPackageShape'] = 1
        modified_info['outerPackageType'] = 0

        # 2. 设置 outerPackageImgUrls 为 imgUrls 的第一个链接
        if modified_info.get('imgUrls') and len(modified_info['imgUrls']) > 0:
            modified_info['outerPackageImgUrls'] = [modified_info['imgUrls'][0]]
        else:
            modified_info['outerPackageImgUrls'] = []

        # 3. 检查是否已有材料或材质属性
        existing_attrs = modified_info.get('attributes', [])
        has_material = False
        for attr in existing_attrs:
            attr_name = attr.get('name', '')
            if '材料' in attr_name or '材质' in attr_name:
                has_material = True
                break

        # 4. 动态构建新属性列表
        new_attributes = [
            {
                "name": "适用年龄段",
                "values": [{"name": "3+", "valueUnit": "", "vid": 73672}],
                "pid": "1141",
                "templatePid": "589613",
                "refPid": "1117"
            },
            {
                "name": "颜色",
                "values": [{"name": "混合色", "valueUnit": "", "vid": 26419}],
                "pid": "13",
                "templatePid": "1253663",
                "refPid": "63"
            }
        ]
        # 如果没有材料/材质属性，才添加“主体材质”
        if not has_material:
            new_attributes.append({
                "name": "主体材质",
                "values": [{"name": "ABS(ABS树脂)", "valueUnit": "", "vid": 1608}],
                "pid": "1",
                "templatePid": "961858",
                "refPid": "1920"
            })

        # 创建新属性的名字到属性的映射
        new_attrs_map = {attr['name']: attr for attr in new_attributes}

        # 处理 attributes
        if modified_info.get('attributes'):
            # 创建原有属性的名字到索引的映射
            existing_attrs_index = {attr['name']: idx for idx, attr in enumerate(modified_info['attributes'])}

            # 遍历新属性
            for attr_name, new_attr in new_attrs_map.items():
                if attr_name in existing_attrs_index:
                    # 如果存在同名属性，则替换
                    modified_info['attributes'][existing_attrs_index[attr_name]] = new_attr
                    print(f"替换属性: {attr_name}")
                else:
                    # 如果不存在同名属性，则新增
                    modified_info['attributes'].append(new_attr)
                    print(f"新增属性: {attr_name}")
        else:
            # 如果没有 attributes，直接设置
            modified_info['attributes'] = new_attributes
            print("新增所有属性")

        # 5. 修改 skuMap 中的所有 SKU
        if modified_info.get('skuMap'):
            for sku_key, sku_value in modified_info['skuMap'].items():
                # 设置 itemNum 为空字符串
                sku_value['itemNum'] = ""
                # 设置尺寸
                sku_value['length'] = "10"
                sku_value['width'] = "10"
                sku_value['height'] = "10"
                # 设置重量
                sku_value['weight'] = "100"
                # 设置 numberOfPieces
                sku_value['numberOfPieces'] = 1

        return modified_info

    async def save(self, goods_info):
        """保存商品信息"""
        # 修改商品信息
        modified_goods_info = self.modify_goods_info(goods_info)

        # 将修改后的数据转换为 JSON 字符串
        site_collect_item_info = json.dumps(modified_goods_info, ensure_ascii=False)

        self.logger.info(f'填写商品信息之后的数据：{modified_goods_info}')

        data_json = {
            'siteCollectItemInfo': site_collect_item_info,
            'site': 'PDDKJ',
        }

        data = await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/saveSiteCollectItemInfo',
            payload=data_json,
        )

        self.logger.info(f'修改商品信息响应：{data}')
        return data

    async def publish(self, detailid,shop_ids):
        """发布"""
        data_json = {
            'detailIds[0]': f'{detailid}',
        }

        for i, shop_id in enumerate(shop_ids):
            data_json[f'shopIds[{i}]'] = shop_id

        data = await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/move_collect/saveMoveCollectTask',
            payload=data_json,
        )
        print('发布响应：',data)
        if data['result'] == 'success':
            self.logger.info(f'{detailid}:发布成功')
            return True
        else:
            self.logger.info(f"{detailid}:发布失败")
            return False

    async def dispatch_publish(self,group,target_goods_ids):

        self.logger.info(f'当前店铺组:{group}')

        # 1️⃣ 取400个已采集数据
        items=await self.get_detial_id()

        # 过滤，只保留我采集的数据
        filtered=[]
        for item in items:
            gid=list(item.keys())[0]
            if gid in target_goods_ids:
                filtered.append(item)

        self.logger.info(f'实际采集成功：{len(filtered)}')

        for item in filtered:
            goods_id=list(item.keys())[0]
            detail_id=item[goods_id]

            try:
                # 2️⃣ 选择店铺（动态）
                await self.selectShop(detail_id,group)
                # 3️⃣ 获取商品信息
                goods_info=await self.get_goods_info(detail_id)
                # 4️⃣ 修改
                await self.save(goods_info)
                # 5️⃣ 发布
                ok=await self.publish(detail_id, group)

                if ok:
                    # ✅ 成功
                    ProductDAO.update_status(goods_id, "success")
                else:
                    ProductDAO.update_status(goods_id, "fail")

            except Exception as e:
                ProductDAO.mark_failed(goods_id)
                self.logger.info(f'发布失败:{goods_id}',e)

        self.logger.info('当前组完成')




