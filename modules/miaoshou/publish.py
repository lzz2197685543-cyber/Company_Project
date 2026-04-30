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

PLAN_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "tmp" / "group_goods_plan.json"



class AutoPublish(BaseClient):
    FAIL_LOG_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "publish_fails.jsonl"

    def __init__(self, job):
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

    def load_group_goods_plan(self):
        """读取 group_goods_plan.json"""
        if not PLAN_FILE.exists():
            self.logger.warning(f"未找到计划文件: {PLAN_FILE}")
            return {}

        try:
            with open(PLAN_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"读取计划文件失败: {e}")
            return {}

    def build_goods_group_map(self, plan: dict):
        """
        把 plan 转成:
        {
            "goods_id1": {"group_idx": 1, "shop_group": [...]},
            "goods_id2": {"group_idx": 2, "shop_group": [...]}
        }
        """
        goods_map = {}

        if not plan:
            return goods_map

        # 兼容两种结构：
        # 1) {"1": {"group": [...], "goods_ids": [...]}, "2": ...}
        # 2) {"groups": [{"group_idx": 1, "shop_group": [...], "goods_ids": [...]}, ...]}
        if isinstance(plan, dict) and "groups" in plan and isinstance(plan["groups"], list):
            for item in plan["groups"]:
                group_idx = item.get("group_idx")
                shop_group = item.get("shop_group", [])
                goods_ids = item.get("goods_ids", [])
                for gid in goods_ids:
                    goods_map[str(gid)] = {
                        "group_idx": group_idx,
                        "shop_group": shop_group
                    }
            return goods_map

        if isinstance(plan, dict):
            for k, v in plan.items():
                if not str(k).isdigit():
                    continue
                if not isinstance(v, dict):
                    continue

                group_idx = int(k)
                shop_group = v.get("group", [])
                goods_ids = v.get("goods_ids", [])

                for gid in goods_ids:
                    goods_map[str(gid)] = {
                        "group_idx": group_idx,
                        "shop_group": shop_group
                    }

        return goods_map

    def get_category_config(self, cid):
        """根据类目ID获取属性配置"""
        cid_str = str(cid)
        config = self.category_config.get(cid_str)

        if config:
            self.logger.info(f"✅ 找到类目 {cid} 的配置，包含 {len(config.get('attributes', []))} 个属性")
            return config
        else:
            self.logger.warning(f"⚠️ 未找到类目 {cid} 的配置，跳过该商品")
            return None

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
                "response": response,
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
                        "类目ID", "类目名称", "记录时间"
                    ])

                writer.writerow([
                    cid,
                    sub_category,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ])

        except Exception as e:
            self.logger.error(f"保存类目信息到 CSV 时出错: {e}")

    def _log_goods_attributes(self, goods_info, attributes):
        """记录商品的属性信息到CSV文件"""
        try:
            goods_attr_file = Path(__file__).resolve().parent.parent.parent / "data" / "goods_attributes.csv"
            goods_attr_file.parent.mkdir(parents=True, exist_ok=True)

            self.logger.debug(f'goods_info: {goods_info}')
            self.logger.debug(f'attributes: {attributes}')

            # 安全地提取商品基础信息
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

            # 写入CSV文件
            file_exists = goods_attr_file.exists()
            with open(goods_attr_file, "a", newline='', encoding="utf-8-sig") as f:
                writer = csv.writer(f)

                if not file_exists:
                    writer.writerow([
                        "商品ID", "商品标题", "类目ID", "属性名称", "属性值", "记录时间"
                    ])

                # 遍历属性并写入
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
        """获取detail_id和goods_id"""
        data_json = {
            'claimPublishShopStatus': 'published',
            'titleType': 'multi',
            'remarkType': 'multi',
            'status': 'notPublished',
            'ownerAccountIds[0]': '187908',  # 187908 是lzzit01的账号   202149是root1  202150是root2
            'pageNo': '1',
            'pageSize': '500',
        }

        data = await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/searchCollectBoxDetail',
            payload=data_json,
        )

        items = []

        for i in data['detailList']:
            detail_id = i['collectBoxDetailId']
            goods_id = i['sourceItemId']

            # 跳过没有 goods_id 的商品
            if not goods_id:
                self.logger.warning(f"商品 {detail_id} 没有 sourceItemId，跳过")
                continue

            # 获取最小类目
            sub_category = i.get('siteAndCatMap', {}).get('PDDKJ', {}).get('breadcrumb', '').split('>')[-1] if i.get(
                'siteAndCatMap', {}).get('PDDKJ', {}).get('breadcrumb') else ''
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

            # 如果配置中没有该类目那么就保存
            # 检查是否有配置
            config = self.get_category_config(cid)
            if config is None:
                self._save_category_info_to_csv(cid, sub_category)


        return items

    async def selectShop(self, detailid, shop_ids):
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
            self.logger.info(f'{detailid}: 选择店铺成功')
            return True
        else:
            self.logger.warning(f'{detailid}: 选择店铺失败 - {data.get("reason", "未知原因")}')
            return False

    async def get_goods_info(self, detailId):
        """获取商品信息"""
        data_json = {
            'detailId': f'{detailId}',
        }

        data = await self.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/getCollectItemInfo',
            payload=data_json,
        )

        goods_info = data.get('siteCollectItemInfo') or data.get('shopCollectItemInfo')

        if not goods_info:
            self.logger.error(f"获取商品信息失败: {data}")
            return None

        self.logger.info(f'成功获取商品信息，商品ID: {goods_info.get("sourceItemId")}')
        return goods_info

    def clean_english_title(self, text):
        """清理标题，保留中英文、数字和常用标点

        Args:
            text: 原始标题

        Returns:
            清理后的标题
        """
        if not text:
            return text

        # 替换全角标点为半角
        replacements = {
            '｜': '|',
            '，': ',',
            '。': '.',
            '；': ';',
            '：': ':',
            '？': '?',
            '！': '!',
            '（': '(',
            '）': ')',
            '【': '[',
            '】': ']',
            '“': '"',
            '”': '"',
            '‘': "'",
            '’': "'",
            '　': ' ',
        }

        for full, half in replacements.items():
            text = text.replace(full, half)

        # 保留：中文、英文、数字、空格、常用标点符号
        # \u4e00-\u9fa5: 中文字符范围
        # a-zA-Z0-9: 英文和数字
        # \s: 空格
        # 常用标点：.,!?;:()[]{}|@#$%&*-_=+/\'"
        allowed_pattern = r'[^\u4e00-\u9fa5a-zA-Z0-9\s\.\,\!\?\;\:\'\"\(\)\[\]\{\}\|\@\#\$\%\&\*\-_\=\+\/\\]'
        text = re.sub(allowed_pattern, '', text)

        # 合并多个空格为单个空格
        text = re.sub(r'\s+', ' ', text)

        # 去除首尾空格
        text = text.strip()

        return text

    def modify_goods_info(self, goods_info, add_guide=False):
        """修改商品信息

        Args:
            goods_info: 原始商品信息
            add_guide: 是否添加说明书URL

        Returns:
            修改后的商品信息，如果没有配置则返回None
        """
        if not goods_info:
            self.logger.error("goods_info 为空，无法修改")
            return None

        # 深拷贝原始数据
        modified_info = copy.deepcopy(goods_info)

        # 获取类目ID
        cid = str(goods_info.get('cid', ''))

        # 根据类目ID获取配置
        config = self.get_category_config(cid)

        # 如果没有配置，直接返回None
        if config is None:
            self.logger.warning(f"类目 {cid} 无配置，跳过该商品")
            return None

        self.logger.info(f"商品类目ID: {cid}, 使用配置修改商品信息")

        # 0. 清理和截断标题
        # 处理中文标题：移除特殊字符
        if 'title' in modified_info and modified_info['title']:
            chinese_title = modified_info['title']
            # 移除中文标题中的非法字符（保留中文、字母、数字、常用标点）
            chinese_title = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\s\.\,\!?;:()\[\]【】｜·\-]', '', chinese_title)
            modified_info['title'] = chinese_title
            if len(chinese_title) > 500:
                modified_info['title'] = chinese_title[:497] + "..."
                print(f"⚠️ 中文标题超过500字符，已截断")

        # 处理英文标题：清理非法字符
        if 'multiLanguageTitleMap' in modified_info and 'en' in modified_info['multiLanguageTitleMap']:
            en_title = modified_info['multiLanguageTitleMap']['en']
            original_length = len(en_title)

            # ========== 新增：检测英文标题是否为中文 ==========
            def is_chinese_title(text):
                """检测文本是否主要为中文"""
                if not text:
                    return False
                # 统计中文字符数量
                chinese_chars = re.findall(r'[\u4e00-\u9fa5]', text)
                chinese_count = len(chinese_chars)
                # 如果中文字符占比超过30%，认为是中文标题
                if len(text) > 0:
                    chinese_ratio = chinese_count / len(text)
                    return chinese_ratio > 0.3
                return False

            # 如果是中文标题，设置为空字符串
            if is_chinese_title(en_title):
                print(f"⚠️ 英文标题检测到中文内容，将清空英文标题")
                print(f"   原标题: {en_title[:100]}...")
                en_title = ""
            else:
                # 清理英文标题
                cleaned_title = self.clean_english_title(en_title)

                if cleaned_title != en_title:
                    print(
                        f"⚠️ 英文标题已清理非法字符（原长度：{original_length}，清理后长度：{len(cleaned_title)}）")
                    print(f"   原标题: {en_title[:100]}...")
                    print(f"   新标题: {cleaned_title[:100]}...")
                    en_title = cleaned_title

                # 检查长度并截断
                if len(en_title) > 500:
                    en_title = en_title[:497] + "..."
                    print(f"⚠️ 英文标题超过500字符，已截断（原长度：{original_length}，截断后：{len(en_title)}）")

            modified_info['multiLanguageTitleMap']['en'] = en_title

        # ========== 截断 saleAttributes 中过长的值 ==========
        if modified_info.get('saleAttributes'):
            for sale_attr in modified_info['saleAttributes']:
                if sale_attr.get('values'):
                    # 使用字典去重（基于name）
                    unique_dict = {}
                    for value in sale_attr['values']:
                        name_value = str(value.get('name', '')) if value.get('name') is not None else ''

                        # 截断过长的值
                        if len(name_value) > 30:
                            original_name = name_value
                            name_value = name_value[:27] + "..."
                            value['name'] = name_value
                            print(f"⚠️ 属性值 '{original_name}' 超过30字符，已截断为 '{name_value}'")

                        # 如果name不在字典中，添加
                        if name_value not in unique_dict:
                            unique_dict[name_value] = value

                    # 替换为去重后的列表
                    sale_attr['values'] = list(unique_dict.values())

                    if len(sale_attr['values']) != len(unique_dict):
                        print(
                            f"✅ 销售属性 '{sale_attr.get('name')}' 已去重，从 {len(sale_attr['values'])} 个减少到 {len(unique_dict)} 个")

        # ========== 只保留第一个销售属性，删除其他的 ==========
        if modified_info.get('saleAttributes') and len(modified_info['saleAttributes']) > 1:
            original_count = len(modified_info['saleAttributes'])
            first_attr = modified_info['saleAttributes'][0]
            modified_info['saleAttributes'] = [first_attr]
            print(f"✅ 销售属性从 {original_count} 个减少到 1 个，保留了: {first_attr.get('name')}")

        # 1. 修改 outerPackage 相关字段
        modified_info['outerPackageShape'] = 1
        modified_info['outerPackageType'] = 0

        # 2. 设置 outerPackageImgUrls 为 imgUrls 的第一个链接
        if modified_info.get('imgUrls') and len(modified_info['imgUrls']) > 0:
            modified_info['outerPackageImgUrls'] = [modified_info['imgUrls'][0]]
        else:
            modified_info['outerPackageImgUrls'] = []

        # 3. 直接使用配置中的属性替换
        category_attributes = config.get('attributes', [])
        if category_attributes:
            modified_info['attributes'] = category_attributes
            self.logger.info(f"已将属性替换为配置中的 {len(category_attributes)} 个属性")
        else:
            self.logger.warning("配置中没有属性，保留原有属性")

        # 4. 根据参数决定是否添加说明书URL
        if add_guide:
            modified_info[
                'productGuideFileUrl'] = "https://earth-rt.chengji-inc.com/app_attach_file/8980547/pddkj/2026-04-13/07dd15a1-17e8-490b-8a42-c0732ca784e2.pdf"
            modified_info['productGuideFileName'] = 'manual.pdf'
            self.logger.info(f"已添加说明书URL")
        else:
            if 'productGuideFileUrl' in modified_info:
                self.logger.info("不添加说明书URL")

        # 5. 修改 skuMap 中的所有 SKU
        if modified_info.get('skuMap'):
            for sku_key, sku_value in modified_info['skuMap'].items():
                sku_value['itemNum'] = ""
                sku_value['length'] = "10"
                sku_value['width'] = "10"
                sku_value['height'] = "10"
                sku_value['weight'] = "100"
                sku_value['numberOfPieces'] = 1

        return modified_info

    async def save(self, goods_info):
        """保存商品信息，失败时自动重试添加说明书"""
        if not goods_info:
            self.logger.error("goods_info 为空，无法保存")
            return {'result': 'fail', 'reason': 'goods_info is None'}

        # 先尝试不添加说明书
        modified_goods_info = self.modify_goods_info(goods_info, add_guide=False)

        # 如果没有配置，直接返回
        if modified_goods_info is None:
            self.logger.warning("商品类目无配置，跳过保存")
            return {'result': 'skip', 'reason': 'no category config'}

        # 执行保存请求
        result = await self._save_request(modified_goods_info)

        # 检查是否需要添加说明书
        if result.get('result') == 'fail':
            reason = result.get('reason', '')

            # 记录失败信息
            if '说明书' not in reason:
                self._record_fail_to_json(goods_info, result)

            # 保存失败商品的属性信息
            # existing_attributes = goods_info.get('attributes', [])
            # self._log_goods_attributes(goods_info, existing_attributes)

            # 如果是缺少说明书，尝试添加后重试
            if '说明书' in reason:
                self.logger.info("检测到需要添加说明书，正在添加说明书后重试...")

                modified_goods_info_with_guide = self.modify_goods_info(goods_info, add_guide=True)

                # 再次检查是否有配置
                if modified_goods_info_with_guide is None:
                    self.logger.warning("添加说明书后仍无配置，跳过")
                    return {'result': 'skip', 'reason': 'no category config'}

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
        if modified_goods_info is None:
            self.logger.error("modified_goods_info 为 None，无法保存")
            return {'result': 'fail', 'reason': 'modified_goods_info is None'}

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

    async def publish(self, detailid, shop_ids):
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

        self.logger.info(f'发布响应：{data}')

        if data['result'] == 'success':
            self.logger.info(f'{detailid}: 发布成功')
            return True
        else:
            self.logger.warning(f"{detailid}: 发布失败 - {data.get('reason', '未知原因')}")
            return False

    async def dispatch_publish_concurrent(self, group, target_goods_ids, items=None, batch_id=None, max_concurrent=3):
        """并发发布商品"""
        self.logger.info(f'当前店铺组: {group}, 并发数: {max_concurrent}')

        # 获取已采集数据
        if items is None:
            items = await self.get_detial_id()

        # 过滤，只保留目标商品
        items_map = {}
        for item in items:
            gid = list(item.keys())[0]
            items_map[gid] = item

        filtered = [items_map[gid] for gid in target_goods_ids if gid in items_map]

        self.logger.info(f'实际采集成功: {len(filtered)}')

        if not filtered:
            self.logger.warning("没有需要发布的商品")
            return


        semaphore = asyncio.Semaphore(max_concurrent)

        success_count = 0
        fail_count = 0
        skip_count = 0

        goods_ids = [list(item.keys())[0] for item in filtered]
        if batch_id and goods_ids:
            self.product_dao.batch_update_worker_id(goods_ids, batch_id)

        async def publish_one(item):
            nonlocal success_count, fail_count, skip_count

            async with semaphore:
                goods_id = list(item.keys())[0]
                detail_id = item[goods_id]['detail_id']
                cid = item[goods_id]['cid']

                try:
                    # 检查是否有配置
                    config = self.get_category_config(cid)
                    if config is None:
                        self.logger.warning(f"商品 {goods_id} 类目 {cid} 无配置，跳过")
                        self.product_dao.mark_failed(goods_id)
                        skip_count += 1
                        return

                    # 选择店铺
                    await self.selectShop(detail_id, group)

                    # 获取商品信息
                    goods_info = await self.get_goods_info(detail_id)
                    if not goods_info:
                        self.logger.error(f"获取商品信息失败: {goods_id}")
                        self.product_dao.mark_failed(goods_id)
                        fail_count += 1
                        return

                    # 修改并保存
                    save_result = await self.save(goods_info)

                    if save_result.get('result') == 'skip':
                        self.logger.warning(f"商品 {goods_id} 跳过保存")
                        self.product_dao.mark_failed(goods_id)
                        skip_count += 1
                        return
                    elif save_result.get('result') != 'success':
                        self.logger.error(f"商品 {goods_id} 保存失败")
                        self.product_dao.mark_failed(goods_id)
                        fail_count += 1
                        return

                    # 发布
                    ok = await self.publish(detail_id, group)

                    if ok:
                        self.product_dao.update_status(goods_id, "published")
                        success_count += 1
                    else:
                        self.product_dao.mark_failed(goods_id)
                        fail_count += 1

                except Exception as e:
                    self.logger.error(f'发布失败：{goods_id} - {e}', exc_info=True)
                    self.product_dao.mark_failed(goods_id)
                    fail_count += 1

        # 并发执行所有商品的发布流程
        await asyncio.gather(*[publish_one(item) for item in filtered])

        self.logger.info(f'当前组完成 - 成功: {success_count}, 失败: {fail_count}, 跳过: {skip_count}')

    async def full_process(self, max_concurrent=3):
        """
        完整的发布流程（支持并发）

        Args:
            max_concurrent: 最大并发数，默认3
        """
        # 1. 读取 plan 文件
        plan = self.load_group_goods_plan()
        if not plan:
            self.logger.warning("没有可用的 group_goods_plan.json，结束")
            return

        # 2. 建立 goods_id -> 店铺组 的映射
        goods_group_map = self.build_goods_group_map(plan)
        if not goods_group_map:
            self.logger.warning("计划文件里没有 goods_id 映射，结束")
            return

        self.logger.info(f"计划文件中共有 {len(goods_group_map)} 个 goods_id 待处理")

        # 3. 获取当前采集箱里的 detail_id / goods_id
        detail_items = await self.get_detial_id()
        if not detail_items:
            self.logger.warning("当前没有可处理的采集箱商品")
            return

        self.logger.info(f"当前采集箱共获取到 {len(detail_items)} 条数据")

        # 4. 构建需要处理的商品列表（过滤掉无配置和无映射的商品）
        tasks_data = []
        skipped_no_config = 0  # 无配置跳过的计数
        skipped_no_mapping = 0  # 无映射跳过的计数

        for item in detail_items:
            goods_id = list(item.keys())[0]
            detail_id = item[goods_id]["detail_id"]
            cid = item[goods_id]['cid']

            # 检查是否有配置
            config = self.get_category_config(cid)
            if config is None:
                self.logger.warning(f"商品 {goods_id} 类目 {cid} 无配置，跳过")
                self.product_dao.mark_failed(goods_id)
                skipped_no_config += 1
                continue

            # 检查是否在计划中
            group_info = goods_group_map.get(goods_id)
            if not group_info:
                self.logger.info(f"goods_id={goods_id} 未匹配到店铺组，跳过")
                self.product_dao.mark_failed(goods_id)
                skipped_no_mapping += 1
                continue

            shop_group = group_info["shop_group"]
            group_idx = group_info.get("group_idx")

            tasks_data.append({
                "goods_id": goods_id,
                "detail_id": detail_id,
                "cid": cid,
                "shop_group": shop_group,
                "group_idx": group_idx
            })

        self.logger.info(
            f"准备处理 {len(tasks_data)} 个商品 "
            f"(无配置跳过: {skipped_no_config}, 无映射跳过: {skipped_no_mapping})"
        )

        if not tasks_data:
            self.logger.warning("没有需要处理的商品")
            return

        # 5. 并发处理所有商品
        semaphore = asyncio.Semaphore(max_concurrent)

        success_count = 0
        fail_count = 0
        skip_count = 0

        async def process_one(task):
            nonlocal success_count, fail_count, skip_count

            async with semaphore:
                goods_id = task["goods_id"]
                detail_id = task["detail_id"]
                cid = task["cid"]
                shop_group = task["shop_group"]
                group_idx = task["group_idx"]

                try:
                    self.logger.info(
                        f"开始处理 goods_id={goods_id}, detail_id={detail_id}, "
                        f"group_idx={group_idx}, shop_group={shop_group}"
                    )

                    # 1. 选择店铺
                    select_ok = await self.select_shop_with_result(detail_id, shop_group)
                    if not select_ok:
                        self.logger.warning(f"goods_id={goods_id} 选择店铺失败，跳过")
                        self.product_dao.mark_failed(goods_id)
                        fail_count += 1
                        return

                    # 2. 获取商品信息
                    goods_info = await self.get_goods_info(detail_id)
                    if not goods_info:
                        self.logger.warning(f"goods_id={goods_id} 获取商品信息失败，跳过")
                        self.product_dao.mark_failed(goods_id)
                        fail_count += 1
                        return

                    # 3. 保存商品信息
                    save_result = await self.save(goods_info)

                    if save_result.get('result') == 'skip':
                        self.logger.warning(f"商品 {goods_id} 跳过保存（类目无配置）")
                        self.product_dao.mark_failed(goods_id)
                        skip_count += 1  # ✅ 修复：跳过计数
                        return
                    elif save_result.get('result') != 'success':
                        self.logger.error(f"商品 {goods_id} 保存失败: {save_result.get('reason', '未知错误')}")
                        self.product_dao.mark_failed(goods_id)
                        fail_count += 1
                        return

                    # 4. 发布
                    publish_ok = await self.publish_with_result(detail_id, shop_group)

                    if publish_ok:
                        self.product_dao.update_status(goods_id, "published")
                        success_count += 1
                        self.logger.info(f"✅ 完成 goods_id={goods_id} 的整套流程")
                    else:
                        self.product_dao.mark_failed(goods_id)
                        fail_count += 1
                        self.logger.warning(f"❌ goods_id={goods_id} 发布失败")

                except Exception as e:
                    self.logger.error(f'处理商品 {goods_id} 时发生异常: {e}', exc_info=True)
                    self.product_dao.mark_failed(goods_id)
                    fail_count += 1

        # 执行并发任务
        await asyncio.gather(*[process_one(task) for task in tasks_data])

        # 输出统计信息（包含所有跳过的情况）
        total_skipped = skipped_no_config + skipped_no_mapping + skip_count
        self.logger.info(
            f"发布流程完成 - "
            f"成功: {success_count}, "
            f"失败: {fail_count}, "
            f"跳过: {total_skipped} "
            f"(预处理无配置: {skipped_no_config}, 预处理无映射: {skipped_no_mapping}, 运行时跳过: {skip_count}), "
            f"总计: {len(detail_items)}"
        )

    async def select_shop_with_result(self, detailid, shop_ids):
        """选择店铺并返回布尔结果"""
        try:
            data_json = {'detailIds[0]': f'{detailid}'}

            for i, shop_id in enumerate(shop_ids):
                data_json[f'shopIds[{i}]'] = shop_id

            data = await self.post(
                'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/claimToShop',
                payload=data_json,
            )

            if data.get('result') == 'success':
                self.logger.info(f'{detailid}: 选择店铺成功')
                return True
            else:
                self.logger.warning(f'{detailid}: 选择店铺失败 - {data.get("reason", "未知原因")}')
                return False
        except Exception as e:
            self.logger.error(f'选择店铺异常: {e}')
            return False

    async def publish_with_result(self, detailid, shop_ids):
        """发布并返回布尔结果"""
        try:
            data_json = {'detailIds[0]': f'{detailid}'}

            for i, shop_id in enumerate(shop_ids):
                data_json[f'shopIds[{i}]'] = shop_id

            data = await self.post(
                'https://erp.91miaoshou.com/api/platform/pddkj/move/move_collect/saveMoveCollectTask',
                payload=data_json,
            )

            if data.get('result') == 'success':
                self.logger.info(f'{detailid}: 发布成功')
                return True
            else:
                self.logger.warning(f"{detailid}: 发布失败 - {data.get('reason', '未知原因')}")
                return False
        except Exception as e:
            self.logger.error(f'发布异常: {e}')
            return False

#
if __name__ == '__main__':
    # 测试代码
    async def test():
        publisher = AutoPublish('auto_listing')
        # 示例：发布指定商品

        await publisher.full_process()


    asyncio.run(test())