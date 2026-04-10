import json
import copy
import asyncio

from core.base_client import BaseClient
from storage.product_dao import ProductDAO
import csv
from datetime import datetime
import re
import time
from pathlib import Path


class AutoPublish(BaseClient):
    FAIL_LOG_FILE = Path(__file__).resolve().parent.parent.parent / "data" /"publish_fails.jsonl"

    def __init__(self,job):
        super().__init__(job)
        self.product_dao = ProductDAO(job)

        # ✅ 加载类目属性配置
        config_path = Path(__file__).resolve().parent.parent.parent / "config" / "category_attributes.json"

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.category_config = json.load(f)
            self.logger.info(f"✅ 已加载 {len(self.category_config)} 个类目的属性配置")
        except FileNotFoundError:
            self.logger.warning(f"⚠️ 配置文件不存在: {config_path}，将使用默认配置")
            self.category_config = {}
        except Exception as e:
            self.logger.error(f"❌ 加载配置文件失败: {e}")
            self.category_config = {}

    def get_category_config(self, cid):
        """根据类目ID获取属性配置"""
        cid_str = str(cid)
        config = self.category_config.get(cid_str)

        if config:
            self.logger.info(f"✅ 找到类目 {cid} 的配置，包含 {len(config.get('attributes', []))} 个属性")
            return config
        else:
            self.logger.warning(f"⚠️ 未找到类目 {cid} 的配置，使用默认配置")
            # return self._get_default_config()
    def _get_default_config(self):
        """获取默认配置"""
        return {
            "attributes": [
                {
                    "name": "适用年龄段",
                    "values": [{"name": "3+", "valueUnit": ""}]
                },
                {
                    "name": "主要材质",
                    "values": [{"name": "ABS", "valueUnit": ""}]
                },
                {
                    "name": "品牌名",
                    "values": [{"name": "UNDER THE BAUBLES", "valueUnit": ""}]
                },
                {
                    "name": "颜色",
                    "values": [{"name": "混合色", "valueUnit": ""}]
                }
            ],

        }

    def _record_fail_to_json(self, goods_info, response):
        """将失败信息追加到 JSON Lines 文件"""
        try:
            # 确保目录存在
            self.FAIL_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

            # 提取商品标题（优先取 title，否则取英文标题）
            title = goods_info.get('title')
            if not title:
                title = goods_info.get('multiLanguageTitleMap', {}).get('en', '')

            # 提取缺失属性（例如从 "产品属性【材料】必填" 中提取 "材料"）
            reason = response.get('reason', '')
            missing_attr = None
            match = re.search(r'【(.*?)】', reason)
            if match:
                missing_attr = match.group(1)

            # 构造记录
            record = {
                "timestamp": time.time(),
                "goods_id": goods_info.get('sourceItemId') or goods_info.get('goods_id'),
                "title": title,
                "cid": goods_info.get('cid'),
                "response": response,  # 完整响应
                "reason": reason,
                "missing_attr": missing_attr
            }

            # 追加写入（每行一个 JSON）
            with open(self.FAIL_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            self.logger.info(f"失败记录已保存到 {self.FAIL_LOG_FILE}")
        except Exception as e:
            self.logger.error(f"保存失败记录时出错: {e}")

    def _save_category_info_to_csv(self, cid, sub_category):
        """将类目信息保存到 CSV 文件"""
        try:
            CATEGORY_INFO_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "category_info.csv"
            CATEGORY_INFO_FILE.parent.mkdir(parents=True, exist_ok=True)

            file_exists = CATEGORY_INFO_FILE.exists()

            with open(CATEGORY_INFO_FILE, "a", newline='', encoding="utf-8-sig") as f:
                writer = csv.writer(f)

                if not file_exists:
                    writer.writerow([
                        "类目ID", "类目名称",  "记录时间"
                    ])


                writer.writerow([
                    cid,
                    sub_category,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ])

        except Exception as e:
            self.logger.error(f"保存类目信息到 CSV 时出错: {e}")

    def _count_goods_by_cid(self, cid):
        """统计指定类目下的商品数量（可选）"""
        try:
            # 这里可以查询数据库或实时统计
            # 暂时返回0，或者你可以从ProductDAO查询
            return 0
        except:
            return 0

    def _log_goods_attributes(self, goods_info, attributes):
        """记录商品的属性信息到CSV文件"""
        try:
            # 1. 确定文件路径并确保目录存在
            goods_attr_file = Path(__file__).resolve().parent.parent.parent / "data" / "goods_attributes.csv"
            goods_attr_file.parent.mkdir(parents=True, exist_ok=True)

            print('goods_info:', goods_info)
            print('attributes:', attributes)

            # 2. 安全地提取商品基础信息，避免KeyError
            goods_id = None
            if goods_info:
                goods_id = goods_info.get('sourceItemId')

            title = ""
            if goods_info:
                title = goods_info.get('title')

            cid = goods_info.get('cid') if goods_info else None

            # 如果没有商品ID或没有属性数据，则记录警告并返回
            if not goods_id:
                self.logger.warning(f"无法获取商品ID，跳过属性记录。goods_info内容: {goods_info}")
                return

            if not attributes:
                self.logger.info(f"商品 {goods_id} 没有属性数据，跳过记录。")
                return

            # 3. 写入CSV文件
            file_exists = goods_attr_file.exists()
            with open(goods_attr_file, "a", newline='', encoding="utf-8-sig") as f:
                writer = csv.writer(f)

                if not file_exists:
                    writer.writerow([
                        "商品ID", "商品标题", "类目ID", "属性名称", "属性值", "记录时间"
                    ])

                # 4. 遍历属性并写入
                for attr in attributes:
                    attr_name = attr.get('name', '')
                    values = attr.get('values', [])
                    if values:
                        for val in values:
                            attr_value = val.get('name', '') if isinstance(val, dict) else str(val)
                            writer.writerow([
                                goods_id, title, cid, attr_name, attr_value,
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            ])
                    else:
                        # 没有具体值的属性也记录一行，属性值为空
                        writer.writerow([
                            goods_id, title, cid, attr_name, "",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        ])
            self.logger.info(f"成功记录商品 {goods_id} 的 {len(attributes)} 个属性到CSV")

        except Exception as e:
            self.logger.error(f"记录商品属性到CSV时发生严重错误: {e}", exc_info=True)

    async def get_detial_id(self):
        """获取detail_id和goods_id，detail_id是给选择店铺，获取商品信息的参数，goods_id我们是为了追踪它的状态，最后更新数据库中商品id的状态信息"""

        data_json = {
            'claimPublishShopStatus': 'published',
            'titleType': 'multi',
            'remarkType': 'multi',
            'status': 'notPublished',#
            'ownerAccountIds[0]': '187908',
            'pageNo': '1',
            'pageSize': '500',
        }

        data= await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/searchCollectBoxDetail',
            payload=data_json,
        )

        items=[]
        # 用于记录已经保存过的类目，避免重复保存
        saved_categories = set()

        for i in data['detailList']:
            detail_id = i['collectBoxDetailId']
            goods_id=i['sourceItemId']
            # 获取最小类目
            sub_category=i.get('siteAndCatMap',{}).get('PDDKJ',{}).get('breadcrumb').split('>')[-1]
            # 获取类目ID
            cid = i.get('siteAndCatMap', {}).get('PDDKJ', {}).get('cid', '')

            item = {
                goods_id: {
                    'detail_id': detail_id,
                    'sub_category': sub_category,
                    'cid': cid
                }
            }

            items.append(item)

            # 🔥 关键：保存类目信息到CSV（每个类目只保存一次）
            if cid and cid not in saved_categories:
                self._save_category_info_to_csv(cid, sub_category)
                saved_categories.add(cid)
                # self.logger.info(f"已保存类目信息: {cid} - {sub_category}")

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

    async def get_goods_info(self, detailId,):
        """获取商品信息"""
        data_json = {
            'detailId': f'{detailId}',
        }

        data = await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/getCollectItemInfo',
            payload=data_json,
        )

        goods_info = data.get('siteCollectItemInfo') or data.get('shopCollectItemInfo')


        self.logger.info('获取商品信息：' + str(goods_info))
        return goods_info

    def modify_goods_info(self, goods_info, add_guide=False):
        """修改商品信息

        Args:
            goods_info: 原始商品信息
            add_guide: 是否添加说明书URL
        """
        # 深拷贝原始数据，避免修改原数据
        modified_info = copy.deepcopy(goods_info)

        # ✅ 获取类目ID
        cid = str(goods_info.get('cid', ''))

        # ✅ 根据类目ID获取配置
        config = self.get_category_config(cid)

        self.logger.info(f"商品类目ID: {cid}, 使用配置修改商品信息")

        # 1. 修改 outerPackage 相关字段
        modified_info['outerPackageShape'] = 1
        modified_info['outerPackageType'] = 0

        # 2. 设置 outerPackageImgUrls 为 imgUrls 的第一个链接
        if modified_info.get('imgUrls') and len(modified_info['imgUrls']) > 0:
            modified_info['outerPackageImgUrls'] = [modified_info['imgUrls'][0]]
        else:
            modified_info['outerPackageImgUrls'] = []

        # 3. ✅ 直接使用配置中的属性替换
        category_attributes = config.get('attributes', [])
        if category_attributes:
            modified_info['attributes'] = category_attributes
            self.logger.info(f"已将属性替换为配置中的 {len(category_attributes)} 个属性")
        else:
            self.logger.warning("配置中没有属性，保留原有属性")

        # 4. 根据参数决定是否添加说明书URL
        if add_guide:
            modified_info[
                'productGuideFileUrl'] = "https://earth-rt.chengji-inc.com/app_attach_file/8980547/pddkj/2026-04-10/04673187-e8ff-4761-8054-459512846c78.pdf"
            print(f"已添加说明书URL: {modified_info['productGuideFileUrl']}")
        else:
            # 如果原数据中有说明书URL，可以选择移除或保留
            if 'productGuideFileUrl' in modified_info:
                print("不添加说明书URL")

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
        """保存商品信息，失败时自动重试添加说明书"""
        # 先尝试不添加说明书
        modified_goods_info = self.modify_goods_info(goods_info, add_guide=False)

        # 执行保存请求
        result = await self._save_request(modified_goods_info)

        # 检查是否需要添加说明书
        if result.get('result') == 'fail':
            reason = result.get('reason', '')

            # 记录失败信息
            self._record_fail_to_json(goods_info, result)

            # 保存失败商品的属性信息
            existing_attributes = goods_info.get('attributes', [])
            self._log_goods_attributes(goods_info, existing_attributes)

            # 如果是缺少说明书，尝试添加后重试
            if '说明书' in reason:
                self.logger.info("检测到需要添加说明书，正在添加说明书后重试...")

                modified_goods_info_with_guide = self.modify_goods_info(goods_info, add_guide=True)
                result = await self._save_request(modified_goods_info_with_guide)

                if result.get('result') == 'success':
                    self.logger.info("添加说明书后保存成功")
                else:
                    self.logger.error(f"添加说明书后仍保存失败: {result.get('reason', '未知错误')}")
                    self._record_fail_to_json(goods_info, result)
            else:
                self.logger.error(f"保存失败: {reason}")

        return result

    async def _save_request(self, modified_goods_info):
        """执行保存请求"""
        site_collect_item_info = json.dumps(modified_goods_info, ensure_ascii=False)

        self.logger.info(f'保存商品信息，类目ID: {modified_goods_info.get("cid")}')

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
            detail_id = item[goods_id]['detail_id']

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
                    self.product_dao.update_status(goods_id, "success")
                else:
                    self.product_dao.update_status(goods_id, "fail")

            except Exception as e:
                self.product_dao.mark_failed(goods_id)
                self.logger.info(f'发布失败:{goods_id}',e)

        self.logger.info('当前组完成')


    async def dispath_publish_concurrent(self,group,target_goods_ids,max_concurrent=3):

        items=await self.get_detial_id()

        # 过滤
        filtered=[]
        for item in items:
            gid=list(item.keys())[0]
            if gid in target_goods_ids:
                filtered.append(item)

        print(len(filtered))

        self.logger.info(f'实际采集成功:{len(filtered)}')

        semaphore=asyncio.Semaphore(max_concurrent)

        async def publish_one(item):
            async with semaphore:
                goods_id=list(item.keys())[0]
                detail_id = item[goods_id]['detail_id']
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
                        self.product_dao.update_status("success",goods_id )
                    else:
                        self.product_dao.mark_failed(goods_id )
                except Exception as e:
                    self.product_dao.mark_failed(goods_id)
                    self.logger.error(f'发布失败：{goods_id}-{e}')
        # 并发执行所有商品的发布流程
        await asyncio.gather(*[publish_one(item) for item in filtered])
        self.logger.info('当前组完成')

# if __name__ == '__main__':
#     a=AutoPublish('auto_listing')
#     asyncio.run(a.dispath_publish_concurrent())



