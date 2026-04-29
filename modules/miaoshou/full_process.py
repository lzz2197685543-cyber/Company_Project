import json
from pathlib import Path
import requests
from datetime import datetime, date
import json
import copy
from pathlib import Path
from utils.logger import get_logger
from utils.cookie_manager import CookieManager
import re
import asyncio
import time

PLAN_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "tmp" / "group_goods_plan.json"


class AutoListing:
    def __init__(self, job):
        self.cookies = {
            "acw_tc": "65c93fbb17767401419061192e4845ae102f241cdb3c145b306e3898b5b0d6",
            "bsClientId": "bs_c5e93c4e-b65c-4430-9634-19a52a65edba",
            "Hm_lvt_7974377ca203df6dcf98836f348d0722": "1776740139",
            "HMACCOUNT": "EFBBE3B84A5557E9",
            "_ga": "GA1.1.1595170734.1776740139",
            "mserp": "b6g012rs507q0s92k9vhl2v3h3",
            "CLID": "24495b878ff74376b76bb56e426900f5.20260421.20270421",
            "_clck": "5mwxeb%5E2%5Eg5e%5E0%5E2302",
            "SM": "T",
            "MUID": "1CC60D9EC2C96F5D1DED1ADCC3556E5A",
            "mserp_sst": "b6g012rs507q0s92k9vhl2v3h3",
            "firstClientId": "c40d4ccd-8c69-4b7e-8304-d39d6cf4fed8",
            "accountId": "8980547",
            "autoLoginToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2NvdW50SWQiOiI4OTgwNTQ3IiwiY2xpZW50SWQiOm51bGwsInN1YkFjY291bnRJZCI6MCwiZXhwaXJlVGltZSI6MTc3NzM0NDk0NCwiZ210Q3JlYXRlIjoiMjAyNi0wNC0yMSAxMDo1NTo0NCIsImV4cCI6MTc3Njc0MDI2NH0.4gRAPSbQV81Dg5eVXYmGb61JCjNspATlbzdn0qM2Hnw",
            "MR": "0",
            "SRM_B": "1CC60D9EC2C96F5D1DED1ADCC3556E5A",
            "_ga_28Q604ZC7E": "GS2.1.s1776740138$o1$g1$t1776740140$j58$l0$h0",
            "_ati": "2159820798311",
            "Hm_lpvt_7974377ca203df6dcf98836f348d0722": "1776740141",
            "_clsk": "piplut%5E1776740141220%5E1%5E0%5En.clarity.ms%2Fcollect"
        }

        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://erp.91miaoshou.com',
            'Referer': 'https://erp.91miaoshou.com/pddkj/collect_box/items',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
            'X-Breadcrumb': 'item-pddkj-collectBox',
            'bx-v': '2.5.11',
            'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'x-app-rhino': 'cae478c1d7b601156408f8f2b5597ad8',
            'x-front-version': '1776309618443',
            'x-timestamp': '1776331368',
            # 'Cookie': '_ati=1013620263311; _ga=GA1.1.1217233704.1773986026; bsClientId=bs_6678ce39-240c-4f85-ad0c-d5254f938866; _nano_fp=Xpm8XqUJn59xl0TyX9_CfZv_Y3DgsZR7jvtpG6A9; Hm_lvt_7974377ca203df6dcf98836f348d0722=1773986025,1776044578; HMACCOUNT=8C4483590F81501B; mserp=ijs9ketfcvtjdnbivu4s92ii86; mserp_sst=ijs9ketfcvtjdnbivu4s92ii86; firstClientId=9a21dfb5-a27c-45a0-8480-c91e04650e6d; accountId=8980547; autoLoginToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2NvdW50SWQiOiI4OTgwNTQ3IiwiY2xpZW50SWQiOm51bGwsInN1YkFjY291bnRJZCI6MCwiZXhwaXJlVGltZSI6MTc3NjY0OTM4NCwiZ210Q3JlYXRlIjoiMjAyNi0wNC0xMyAwOTo0MzowNCIsImV4cCI6MTc3NjA0NDcwNH0.gL57YqbBYtdHklS7M2Q7VmVnbILEcZoSp7BGvHHzc4c; tfstk=gIMnqt2-oXPByZ_wnayB6p2iy4ROiJw8Uuu8yznzzk4_8XF8efva2VmpAJUdq4mYWvUKvJozqqgI2y8Qp3xok46JRQwd4V0K0XFLJYc1EmiSpHwJAJiQF88vkKeoADwSfes9pv2NbkZWeTPe6Ru7Et6wkKpxYa628_8x9GG08uqzUkzUUP-g0u5zU7rEbhqz2w5PL4owjPZlTwPzTO7a0oQzU8urbhq77zyzT4owjuaazbRNUrDrCA82aFNzEP5VwkF3uDzGncMaYKz6hrWPqAqnj-mFgTWrIkVnItWNRNFKamNjp0vlCJnm_RroFn5urScrpRke7KP_aAuab2KRNlco4xwQ9g6SoJ0ugvPG4toQEywgbx-RGPHgJ4k3_nB0wRkYgJl91eF8Ko0rd2jlUqotDvNsEEbaPXEbQuDW0_4uagJF_sWexTZw2A55NWr_jrdYKQ0QAUgcChxGGBNUfkLvjhf5NZ6PnEKMjs_7TlZpk; _clck=17qamnf%5E2%5Eg59%5E0%5E2270; Hm_lpvt_7974377ca203df6dcf98836f348d0722=1776311155; _ga_28Q604ZC7E=GS2.1.s1776309853$o42$g1$t1776311208$j6$l0$h0; acw_tc=65c93fbb17763310215554678e1b446008f83057425d99393d0355b3489afa; _clsk=1vchric%5E1776331024614%5E1%5E0%5Ee.clarity.ms%2Fcollect',
        }

        self.logger = get_logger(job)

        self.cookies_manager = CookieManager(job)
        self.FAIL_LOG_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "publish_fails1.jsonl"

        # ✅ 加载类目属性配置
        config_path = Path(__file__).resolve().parent.parent.parent / "config" / "category_attributes.json"

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.category_config = json.load(f)
            self.logger.info(f"✅ 已加载 {len(self.category_config)} 个类目的属性配置")
        except FileNotFoundError:
            self.logger.error(f"⚠️ 配置文件不存在: {config_path}，将使用默认配置")
            self.category_config = {}
        except Exception as e:
            self.logger.error(f"❌ 加载配置文件失败: {e}")
            self.category_config = {}

    async def get_cookie(self):
        # await self.cookies_manager.refresh()
        self.cookies = await self.cookies_manager.get_auth()

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

    def get_category_config(self, cid):
        """根据类目ID获取属性配置"""
        cid_str = str(cid)
        config = self.category_config.get(cid_str)

        if config:
            self.logger.info(f"✅ 找到类目 {cid} 的配置，包含 {len(config.get('attributes', []))} 个属性")
            return config
        else:
            self.logger.info(f"⚠️ 未找到类目 {cid} 的配置，使用默认配置")
            return None
            # return self._get_default_config()


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


    def get_detial_id(self):
        """获取 detail_id 和 goods_id"""
        data_json = {
            'claimPublishShopStatus': 'published',
            'titleType': 'multi',
            'remarkType': 'multi',
            'status': 'notPublished',
            'ownerAccountIds[0]': '187908',
            'ownerAccountIds[1]': '202149',
            'ownerAccountIds[2]': '202150',
            'pageNo': '1',
            'pageSize': '500',
        }

        data = requests.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/searchCollectBoxDetail',
            headers=self.headers,
            cookies=self.cookies,
            data=data_json,
        ).json()

        items = []

        for i in data.get('detailList', []):
            goods_id = i.get('sourceItemId')
            detail_id = i.get('collectBoxDetailId')

            if not goods_id or not detail_id:
                continue

            breadcrumb = i.get('siteAndCatMap', {}).get('PDDKJ', {}).get('breadcrumb', '')
            sub_category = breadcrumb.split('>')[-1] if breadcrumb else ''
            cid = i.get('siteAndCatMap', {}).get('PDDKJ', {}).get('cid', '')

            items.append({
                "goods_id": str(goods_id),
                "detail_id": detail_id,
                "sub_category": sub_category,
                "cid": cid
            })

        return items

    def selectShop(self, detailid, shop_ids):
        """选择店铺"""
        self.logger.info(f"detailid是: {detailid}, shop_ids: {shop_ids}")

        data = {
            'detailIds[0]': f'{detailid}',
        }

        for i, shop_id in enumerate(shop_ids):
            data[f'shopIds[{i}]'] = shop_id

        response = requests.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/claimToShop',
            cookies=self.cookies,
            headers=self.headers,
            data=data,
        )

        try:
            resp_json = response.json()
        except Exception:
            self.logger.error(f"选择店铺返回非 JSON: {response.text[:200]}")
            return False

        self.logger.info(f"选择店铺响应：{resp_json}")
        if resp_json.get('result') == 'success':
            self.logger.info('选择店铺成功')
            return True

        self.logger.info(f"选择店铺失败: {resp_json}")
        return False

    def get_goods_info(self, detailId):
        """获取商品id"""
        data = {
            'detailId': f'{detailId}',
        }

        response = requests.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/getCollectItemInfo',
            cookies=self.cookies,
            headers=self.headers,
            data=data,
        )

        self.logger.info(f"获取商品响应：{response.json()}")

        goods_info = response.json()['siteCollectItemInfo']
        self.logger.info(f"商品信息: {json.dumps(goods_info, ensure_ascii=False)}")
        self.logger.info(f"属性： {goods_info['attributes']}")
        return goods_info

    def save(self, goods_info):
        """保存商品信息"""
        # 修改商品信息（先不添加说明书）
        modified_goods_info = self.modify_goods_info(goods_info, add_guide=False)

        # 尝试保存
        result = self._save_request(modified_goods_info)

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
                    self.logger.info("添加说明书后仍无配置，跳过")
                    return {'result': 'skip', 'reason': 'no category config'}

                result = self._save_request(modified_goods_info_with_guide)

                if result.get('result') == 'success':
                    self.logger.info("添加说明书后保存成功")
                else:
                    self.logger.info(f"添加说明书后仍保存失败: {result.get('reason', '未知错误')}")
                    self._record_fail_to_json(goods_info, result)
            else:
                self.logger.info(f"保存失败: {reason}")

        return result

    def _save_request(self, modified_goods_info):
        """执行保存请求"""
        # 将修改后的数据转换为 JSON 字符串
        site_collect_item_info = json.dumps(modified_goods_info, ensure_ascii=False)
        self.logger.info(site_collect_item_info)

        data = {
            'siteCollectItemInfo': site_collect_item_info,
            'site': 'PDDKJ',
        }

        response = requests.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/collect_box/saveSiteCollectItemInfo',
            cookies=self.cookies,
            headers=self.headers,
            data=data,
        )

        self.logger.info(f"保存响应: {response.json()}")
        return response.json()

    def clean_english_title(self, text):
        """清理英文标题，只保留英文、数字和基本标点符号

        Args:
            text: 原始标题

        Returns:
            清理后的英文标题
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
            '、': ',',  # 中文顿号转逗号
            '…': '...',  # 省略号转三个点
        }

        for full, half in replacements.items():
            text = text.replace(full, half)

        # 移除所有中文字符（\u4e00-\u9fa5 是中文字符范围）
        text = re.sub(r'[\u4e00-\u9fa5]', '', text)

        # 只保留：英文字母、数字、空格、常用标点符号
        # a-zA-Z0-9: 英文和数字
        # \s: 空格
        # 常用标点：.,!?;:()[]{}|@#$%&*-_=+/'" 和连字符-
        allowed_pattern = r'[^a-zA-Z0-9\s\.\,\!\?\;\:\'\"\(\)\[\]\{\}\|\@\#\$\%\&\*\-_\=\+\/\\\\]'
        text = re.sub(allowed_pattern, '', text)

        # 合并多个空格为单个空格
        text = re.sub(r'\s+', ' ', text)

        # 去除首尾空格和标点
        text = text.strip('.,!?;: ')

        # 如果清理后为空，返回空字符串
        if not text or len(text.strip()) == 0:
            print(f"⚠️ 英文标题清理后为空")
            return ""

        return text


    def modify_goods_info(self, goods_info, add_guide=False):
        """修改商品信息

        Args:
            goods_info: 原始商品信息
            add_guide: 是否添加说明书URL

        Returns:
            修改后的商品信息，如果没有配置则返回None
        """
        # 深拷贝原始数据
        modified_info = copy.deepcopy(goods_info)

        # 获取类目ID
        cid = str(goods_info.get('cid', ''))

        # 根据类目ID获取配置
        config = self.get_category_config(cid)

        # 如果没有配置，直接返回None
        if config is None:
            self.logger.info(f"⚠️ 类目 {cid} 无配置，跳过该商品")
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
                self.logger.info(f"⚠️ 中文标题超过500字符，已截断")

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
                self.logger.info(f"⚠️ 英文标题检测到中文内容，将清空英文标题")
                self.logger.info(f"   原标题: {en_title[:100]}...")
                en_title = ""
            else:
                # 清理英文标题
                cleaned_title = self.clean_english_title(en_title)

                if cleaned_title != en_title:
                    self.logger.info(f"⚠️ 英文标题已清理非法字符（原长度：{original_length}，清理后长度：{len(cleaned_title)}）")
                    self.logger.info(f"   原标题: {en_title[:100]}...")
                    self.logger.info(f"   新标题: {cleaned_title[:100]}...")
                    en_title = cleaned_title

                # 检查长度并截断
                if len(en_title) > 500:
                    en_title = en_title[:497] + "..."
                    self.logger.info(f"⚠️ 英文标题超过500字符，已截断（原长度：{original_length}，截断后：{len(en_title)}）")

            modified_info['multiLanguageTitleMap']['en'] = en_title

        # ========== 截断 saleAttributes 中过长的值 ==========
        # ========== 处理 saleAttributes 并去重 ==========
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
            self.logger.info(f"✅ 销售属性从 {original_count} 个减少到 1 个，保留了: {first_attr.get('name')}")

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
            self.logger.info("配置中没有属性，保留原有属性")

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

    def publish(self, detailid, shop_ids):
        """发布"""
        data = {
            'detailIds[0]': f'{detailid}',
        }

        for i, shop_id in enumerate(shop_ids):
            data[f'shopIds[{i}]'] = shop_id

        response = requests.post(
            'https://erp.91miaoshou.com/api/platform/pddkj/move/move_collect/saveMoveCollectTask',
            cookies=self.cookies,
            headers=self.headers,
            data=data,
        )

        try:
            resp_json = response.json()
        except Exception:
            self.logger.error(f"发布返回非 JSON: {response.text[:200]}")
            return False

        self.logger.info(f"发布响应：{resp_json}")
        if resp_json.get('result') == 'success':
            self.logger.info('发布成功')
            return True

        self.logger.info(f"发布失败: {resp_json}")
        return False

    async def run(self):
        await self.get_cookie()

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
        detail_items = self.get_detial_id()
        if not detail_items:
            self.logger.warning("当前没有可处理的采集箱商品")
            return

        self.logger.info(f"当前采集箱共获取到 {len(detail_items)} 条数据")

        # 4. 逐个处理：按 goods_id 找到应该上的店铺组
        for item in detail_items:
            goods_id = str(item["goods_id"])
            detail_id = item["detail_id"]
            cid=item['cid']

            # 检查是否有配置
            config = self.get_category_config(cid)
            if config is None:
                self.logger.warning(f"商品 {goods_id} 类目 {cid} 无配置，跳过")
                return

            group_info = goods_group_map.get(goods_id)
            if not group_info:
                self.logger.info(f"goods_id={goods_id} 未匹配到店铺组，跳过")
                continue

            shop_group = group_info["shop_group"]
            group_idx = group_info.get("group_idx")

            self.logger.info(
                f"开始处理 goods_id={goods_id}, detail_id={detail_id}, group_idx={group_idx}, shop_group={shop_group}"
            )

            # 选择店铺
            ok = self.selectShop(detail_id, shop_group)
            if not ok:
                self.logger.warning(f"goods_id={goods_id} 选择店铺失败，跳过")
                continue

            # 获取商品信息
            goods_info = self.get_goods_info(detail_id)
            if not goods_info:
                self.logger.warning(f"goods_id={goods_id} 获取商品信息失败，跳过")
                continue

            # 保存商品信息
            save_result = self.save(goods_info)
            if not save_result or save_result.get('result') != 'success':
                self.logger.warning(f"goods_id={goods_id} 保存失败，跳过发布")
                continue

            # 发布
            publish_ok = self.publish(detail_id, shop_group)
            if not publish_ok:
                self.logger.warning(f"goods_id={goods_id} 发布失败")
                continue

            self.logger.info(f"✅ 完成 goods_id={goods_id} 的整套流程")

if __name__ == '__main__':
    a = AutoListing("auto_listing_full")
    asyncio.run(a.run())