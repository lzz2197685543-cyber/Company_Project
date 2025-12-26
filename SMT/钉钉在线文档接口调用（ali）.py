import os
import json
import time
from alibabacloud_dingtalk.notable_1_0.client import Client as NotableClient
from alibabacloud_dingtalk.oauth2_1_0.client import Client as Oauth2Client
from alibabacloud_dingtalk.oauth2_1_0 import models as oauth2_models
from alibabacloud_dingtalk.notable_1_0 import models as notable_models
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models


class DingTalkNotableClient:
    def __init__(self, config_path,operator_id):
        """初始化钉钉文档客户端"""
        self.config = self._load_config(config_path)
        self.operator_id = operator_id
        self.access_token = self._get_token()

    def _load_config(self, config_path):
        """加载配置文件"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        dingding = config.get('dingding', {})
        return {
            'client_id': dingding.get('Client ID'),
            'client_secret': dingding.get('Client Secret'),
        }

    def _get_token(self):
        """获取access token，优先使用缓存"""
        cache_file = './data/token_cache.json'

        # 检查缓存
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                if cache.get('expires_at', 0) > time.time():
                    return cache['access_token']
            except:
                pass

        # 刷新token
        return self._refresh_token()

    def _refresh_token(self):
        """刷新access token并缓存"""
        client = Oauth2Client(open_api_models.Config(protocol='https', region_id='central'))
        request = oauth2_models.GetAccessTokenRequest(
            app_key=self.config['client_id'],
            app_secret=self.config['client_secret']
        )

        response = client.get_access_token(request)
        access_token = response.body.access_token
        expires_in = response.body.expire_in or 7200

        # 缓存token
        cache_data = {
            'access_token': access_token,
            'expires_at': time.time() + expires_in
        }
        os.makedirs(os.path.dirname('./data/token_cache.json'), exist_ok=True)
        with open('./data/token_cache.json', 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2)

        return access_token

    def _create_client(self):
        """创建API客户端"""
        return NotableClient(open_api_models.Config(protocol='https', region_id='central'))

    def _handle_token_expired(self, func, *args, **kwargs):
        """处理token过期的通用方法"""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if any(keyword in str(e).lower() for keyword in ['token', '401', 'unauthorized']):
                # token过期，刷新后重试
                self.access_token = self._refresh_token()
                return func(*args, **kwargs)
            raise

    def list_sheets(self, app_token: str):
        """列出文档中的所有工作表"""
        client = self._create_client()
        headers = notable_models.GetAllSheetsHeaders()
        headers.x_acs_dingtalk_access_token = self.access_token
        request = notable_models.GetAllSheetsRequest(operator_id=self.operator_id)

        def _list_sheets():
            response = client.get_all_sheets_with_options(
                app_token, request, headers, util_models.RuntimeOptions()
            )
            if response.body and hasattr(response.body, 'to_map'):
                return response.body.to_map()
            return {}

        return self._handle_token_expired(_list_sheets)

    def get_sheet_info(self, app_token: str, sheet_name: str):
        """获取工作表详细信息"""
        client = self._create_client()
        headers = notable_models.GetSheetHeaders()
        headers.x_acs_dingtalk_access_token = self.access_token
        request = notable_models.GetSheetRequest(operator_id=self.operator_id)

        def _get_sheet():
            response = client.get_sheet_with_options(
                app_token, sheet_name, request, headers, util_models.RuntimeOptions()
            )
            if response.body and hasattr(response.body, 'to_map'):
                return response.body.to_map()
            return {}

        return self._handle_token_expired(_get_sheet)

    def list_records(self, app_token: str, sheet_name: str, max_results: int = 100, next_token: str = None):
        """获取数据表中的记录

        Args:
            app_token: 文档ID
            sheet_name: 工作表名称
            max_results: 每页记录数，默认100
            next_token: 分页令牌
        """
        client = self._create_client()
        headers = notable_models.ListRecordsHeaders()
        headers.x_acs_dingtalk_access_token = self.access_token

        request = notable_models.ListRecordsRequest(
            operator_id=self.operator_id,
            max_results=max_results
        )

        if next_token:
            request.next_token = next_token

        def _list_records():
            response = client.list_records_with_options(
                app_token, sheet_name, request, headers, util_models.RuntimeOptions()
            )
            if response.body and hasattr(response.body, 'to_map'):
                return response.body.to_map()
            return {}

        return self._handle_token_expired(_list_records)

    def get_all_records(self, app_token: str, sheet_name: str, batch_size: int = 100):
        """获取所有记录（自动处理分页）"""
        all_records = []
        next_token = None

        while True:
            result = self.list_records(app_token, sheet_name, batch_size, next_token)

            # 尝试从不同字段获取数据
            records = None
            for key in ['values', 'records', 'data']:
                if key in result and isinstance(result[key], list):
                    records = result[key]
                    break

            if records:
                all_records.extend(records)

            # 检查是否还有更多数据
            if result.get('hasMore') and result.get('nextToken'):
                next_token = result['nextToken']
                time.sleep(0.5)  # 避免请求过快
            else:
                break

        return all_records


def main():
    """使用示例"""
    try:
        operator_id="ZiSpuzyA49UNQz7CvPBUvhwiEiE"
        # 配置
        app_token = 'XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL'  # 文档ID
        sheet_name = '销量与库存-日更'

        # ./data/shopee_accounts.json 配置文件路径

        # 初始化客户端
        client = DingTalkNotableClient('./data/config.json',operator_id)
        print(f"操作者ID: {client.operator_id}")

        # 1. 列出所有工作表
        print("\n=== 列出所有工作表 ===")
        sheets_result = client.list_sheets(app_token)
        sheets = sheets_result.get('value', sheets_result.get('sheets', []))
        for sheet in sheets[:5]:  # 显示前5个
            if isinstance(sheet, dict):
                name = sheet.get('name', sheet.get('title', '未知'))
                print(f"- {name}")

        # 2. 获取工作表信息
        print(f"\n=== 获取工作表 '{sheet_name}' 信息 ===")
        sheet_info = client.get_sheet_info(app_token, sheet_name)
        print(f"工作表结构: {json.dumps(sheet_info, ensure_ascii=False, indent=2)}")

        # 3. 获取所有记录
        print(f"\n=== 获取 '{sheet_name}' 的所有记录 ===")
        records = client.get_all_records(app_token, sheet_name, batch_size=50)
        print(f"找到 {len(records)} 条记录")

        if records:
            # 显示前3条记录
            print(f"\n前3条记录:")
            for i, record in enumerate(records[:3], 1):
                print(f"\n记录 {i}:")
                print(json.dumps(record, ensure_ascii=False, indent=2, default=str))


    except Exception as e:
        print(f"执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()