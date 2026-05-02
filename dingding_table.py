import json
import os
from datetime import datetime
import requests
from typing import Dict, Any, List, Optional, Union, Callable
import time
from pathlib import Path

"""上传/删除/查询钉钉多维表的数据"""

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "config.json"
token_cache = Path(__file__).resolve().parent.parent.parent / "token_cache.json"


class DingTalkTokenManager:
    """钉钉Token管理器"""

    def __init__(self, config_path: str = CONFIG_DIR, token_cache_path: str = token_cache):
        """
        初始化Token管理器

        Args:
            config_path: 配置文件路径
            token_cache_path: Token缓存文件路径
        """
        self.config_path = config_path
        self.token_cache_path = token_cache_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"配置文件未找到: {self.config_path}")
            return {}
        except json.JSONDecodeError as e:
            print(f"配置文件格式错误: {e}")
            return {}

    def get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        """
        获取有效的access_token

        Args:
            force_refresh: 是否强制刷新token

        Returns:
            access_token字符串，获取失败返回None
        """
        # 如果强制刷新或者没有缓存文件，直接获取新token
        if force_refresh or not os.path.exists(self.token_cache_path):
            return self._refresh_access_token()

        # 读取缓存的token
        try:
            with open(self.token_cache_path, 'r', encoding='utf-8') as f:
                token_data = json.load(f)

            access_token = token_data.get("access_token")
            expires_at = token_data.get("expires_at")

            # 检查token是否有效（提前5分钟过期）
            if access_token and expires_at and time.time() < expires_at - 300:
                print("使用缓存的access_token")
                return access_token
            else:
                print("token已过期，重新获取")
                return self._refresh_access_token()

        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            print(f"读取token缓存失败: {e}")
            return self._refresh_access_token()

    def _refresh_access_token(self) -> Optional[str]:
        """
        刷新access_token并保存到缓存

        Returns:
            access_token字符串，获取失败返回None
        """
        appkey = self.config.get("dingding", {}).get("Client ID")
        appsecret = self.config.get("dingding", {}).get("Client Secret")

        if not appkey or not appsecret:
            print("配置文件中缺少Client ID或Client Secret")
            return None

        url = f"https://oapi.dingtalk.com/gettoken?appkey={appkey}&appsecret={appsecret}"

        try:
            response = requests.get(url, timeout=10)
            result = response.json()

            if result.get('errcode') == 0:
                access_token = result.get('access_token')
                expires_in = result.get('expires_in', 7200)  # 默认2小时

                # 计算过期时间戳
                expires_at = time.time() + expires_in

                # 保存到缓存文件
                token_data = {
                    "access_token": access_token,
                    "expires_at": expires_at,
                    "refresh_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "expires_in": expires_in
                }

                # 确保目录存在
                os.makedirs(os.path.dirname(self.token_cache_path), exist_ok=True)

                with open(self.token_cache_path, 'w', encoding='utf-8') as f:
                    json.dump(token_data, f, indent=2, ensure_ascii=False)

                print(f"✅ 成功获取新的access_token，有效期{expires_in}秒")
                print(f"保存到: {self.token_cache_path}")
                return access_token
            else:
                print(f"❌ 获取token失败")
                print(f"错误码: {result.get('errcode')}")
                print(f"错误信息: {result.get('errmsg')}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"请求token出错: {e}")
            return None
        except Exception as e:
            print(f"刷新token异常: {e}")
            return None


class DingTalkDocClient:
    def __init__(self, token_manager: DingTalkTokenManager):
        """
        初始化钉钉文档客户端

        Args:
            token_manager: Token管理器实例
        """
        self.token_manager = token_manager
        self.base_url = "https://api.dingtalk.com"
        self._update_headers()

    def _update_headers(self):
        """更新请求头中的token"""
        access_token = self.token_manager.get_access_token()
        if access_token:
            self.headers = {
                "x-acs-dingtalk-access-token": access_token,
                "Content-Type": "application/json"
            }
            return True
        return False

    def _handle_token_expired(self) -> bool:
        """
        处理token过期情况

        Returns:
            是否成功刷新token
        """
        print("Token可能已过期，尝试刷新...")
        return self._update_headers()

    def _make_request(self, method: str, url: str, **kwargs) -> Optional[dict]:
        """
        发送请求，自动处理token刷新

        Args:
            method: 请求方法
            url: 请求URL
            **kwargs: 其他请求参数

        Returns:
            响应数据，失败返回None
        """
        # 最大重试次数
        max_retries = 2
        retry_count = 0

        while retry_count < max_retries:
            try:
                # 确保headers存在
                if not hasattr(self, 'headers') or not self.headers:
                    if not self._update_headers():
                        print("无法获取有效的access token")
                        return None

                # 发送请求
                response = requests.request(method, url, headers=self.headers, **kwargs)

                # 检查是否是token过期
                if response.status_code == 401:
                    print(f"⚠️ 收到401错误，token可能已过期")
                    if retry_count < max_retries - 1:
                        # 强制刷新token
                        self.token_manager.get_access_token(force_refresh=True)
                        self._update_headers()
                        retry_count += 1
                        print(f"第{retry_count}次重试...")
                        continue
                    else:
                        print("重试次数已达上限")
                        return None

                # 检查请求是否成功
                if response.status_code == 200:
                    return response.json()
                else:
                    print(f"❌ 请求失败，状态码: {response.status_code}")
                    print(f"❌ 错误信息: {response.text}")
                    return None

            except requests.exceptions.RequestException as e:
                print(f"请求异常: {e}")
                return None

        return None

    def get_all_sheets(self, workbook_id: str, operator_id: str) -> Optional[Dict[str, Any]]:
        """
        获取表格所有工作表

        Args:
            workbook_id: 表格文档ID
            operator_id: 操作者ID（用户unionid）

        Returns:
            返回所有工作表信息的字典，失败返回None
        """
        url = f"{self.base_url}/v1.0/doc/workbooks/{workbook_id}/sheets"

        params = {
            "operatorId": operator_id
        }

        print(f"\n📋 尝试获取工作表列表:")
        print(f"  workbook_id: {workbook_id}")
        print(f"  operator_id: {operator_id}")

        result = self._make_request('GET', url, params=params)

        # 打印完整的返回结果用于调试
        if result:
            print(f"✅ API返回成功")
            print(f"返回数据结构: {list(result.keys())}")
        else:
            print(f"❌ API返回失败")

        return result

    def get_sheet_by_id(self, workbook_id: str, sheet_id: str, operator_id: str) -> Optional[dict]:
        """
        获取指定工作表信息

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID

        Returns:
            返回工作表信息，失败返回None
        """
        url = f"{self.base_url}/v1.0/doc/workbooks/{workbook_id}/sheets/{sheet_id}"

        params = {
            "operatorId": operator_id
        }

        return self._make_request('GET', url, params=params)

    # ==================== 修正：使用正确的表格API ====================

    def get_range_data(self, workbook_id: str, sheet_id: str, operator_id: str,
                       range_address: str) -> Optional[dict]:
        """
        获取指定范围的数据（修正后的方法）

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            range_address: 范围地址，如'A1'

        Returns:
            范围数据
        """
        # 注意：这里使用的是正确的API路径
        url = f"{self.base_url}/v1.0/doc/workbooks/{workbook_id}/sheets/{sheet_id}/ranges/{range_address}"

        params = {
            "operatorId": operator_id
        }

        return self._make_request('GET', url, params=params)

    def update_range_data(self, workbook_id: str, sheet_id: str, operator_id: str,
                          range_address: str, values: List[List[Any]]) -> Optional[dict]:
        """
        更新指定范围的数据

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            range_address: 范围地址，如'A1:B2'
            values: 要更新的数据，二维数组

        Returns:
            更新结果
        """
        url = f"{self.base_url}/v1.0/doc/workbooks/{workbook_id}/sheets/{sheet_id}/ranges/{range_address}"

        body = {
            "operatorId": operator_id,
            "values": values
        }

        return self._make_request('PUT', url, json=body)

    def insert_rows(self, workbook_id: str, sheet_id: str, operator_id: str,
                    row: int, row_count: int = 1) -> Optional[dict]:
        """
        插入行（修正后的方法）

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            row: 行号（从0开始，0表示在第一行前插入）
            row_count: 插入的行数

        Returns:
            插入结果
        """
        # 注意：这里的API路径是/insertRowsBefore，不是/insertRowsBefore/
        url = f"{self.base_url}/v1.0/doc/workbooks/{workbook_id}/sheets/{sheet_id}/insertRowsBefore"

        body = {
            "operatorId": operator_id,
            "row": row,
            "rowCount": row_count
        }

        return self._make_request('POST', url, json=body)

    # ==================== 新增方法：空行插入数据相关功能 ====================

    def find_first_empty_row_batch(self, workbook_id: str, sheet_id: str, operator_id: str,
                                   start_row: int = 1, max_rows: int = 1000,
                                   check_column: str = 'A', batch_size: int = 50) -> Optional[int]:
        """
        批量查找第一个空行（优化版）

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            start_row: 开始查找的行
            max_rows: 最大查找行数
            check_column: 检查的列
            batch_size: 每批获取的行数

        Returns:
            第一个空行的行号
        """
        print(f"\n🔍 批量查找第一个空行，从第{start_row}行开始...")

        for batch_start in range(start_row, start_row + max_rows, batch_size):
            batch_end = min(batch_start + batch_size - 1, start_row + max_rows - 1)

            # 批量获取一个范围的数据
            range_addr = f"{check_column}{batch_start}:{check_column}{batch_end}"
            result = self.get_range_data(workbook_id, sheet_id, operator_id, range_addr)

            if result and result.get('values'):
                values = result['values']

                # 检查这一批中的每一行
                for i, row_value in enumerate(values):
                    current_row = batch_start + i
                    cell_value = row_value[0] if row_value and len(row_value) > 0 else None

                    if not cell_value or not str(cell_value).strip():
                        print(f"✅ 找到空行: 第{current_row}行")
                        return current_row
            else:
                # 如果返回为空，说明这批数据都是空的，直接返回第一行
                print(f"✅ 找到空行: 第{batch_start}行")
                return batch_start

            print(f"  已检查到第{batch_end}行，继续...")

        print(f"⚠️ 在{max_rows}行内没有找到空行")
        return None

    def find_last_data_row(self, workbook_id: str, sheet_id: str, operator_id: str,
                           check_column: str = 'A', max_rows: int = 1000) -> int:
        """
        找到最后一行有数据的行号（二分查找法）

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            check_column: 检查的列
            max_rows: 最大行数

        Returns:
            最后有数据的行号
        """
        print(f"\n🔍 使用二分法查找最后数据行...")

        # 二分查找
        left, right = 1, max_rows
        last_data_row = 0

        while left <= right:
            mid = (left + right) // 2

            # 检查中间行
            range_addr = f"{check_column}{mid}"
            result = self.get_range_data(workbook_id, sheet_id, operator_id, range_addr)

            if result and result.get('values') and result['values']:
                cell_value = result['values'][0][0] if result['values'][0] else None
                if cell_value and str(cell_value).strip():
                    # 有数据，向上查找
                    last_data_row = mid
                    left = mid + 1
                else:
                    # 无数据，向下查找
                    right = mid - 1
            else:
                # 无数据，向下查找
                right = mid - 1

        print(f"✅ 最后数据行: {last_data_row}")
        return last_data_row

    def find_first_empty_row_optimized(self, workbook_id: str, sheet_id: str, operator_id: str,
                                       check_column: str = 'A', max_rows: int = 1000) -> Optional[int]:
        """
        优化的空行查找：先找最后数据行，然后返回下一行

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            check_column: 检查的列
            max_rows: 最大行数

        Returns:
            第一个空行的行号
        """
        # 找到最后有数据的行
        last_row = self.find_last_data_row(workbook_id, sheet_id, operator_id, check_column, max_rows)

        # 下一行就是空行
        empty_row = last_row + 1
        print(f"✅ 第一个空行是第{empty_row}行")

        return empty_row

    def find_first_empty_row(self, workbook_id: str, sheet_id: str, operator_id: str,
                             start_row: int = 1, max_rows: int = 1000,
                             check_column: str = 'A') -> Optional[int]:
        """
        找到第一个空行的行号

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            start_row: 开始查找的行（从1开始）
            max_rows: 最大查找行数
            check_column: 检查哪一列来判断是否为空，默认'A'列

        Returns:
            第一个空行的行号，如果没找到返回None
        """
        print(f"\n🔍 开始查找第一个空行，从第{start_row}行开始，检查{check_column}列...")

        for row in range(start_row, start_row + max_rows):
            # 检查当前行的指定列
            range_addr = f"{check_column}{row}"
            result = self.get_range_data(workbook_id, sheet_id, operator_id, range_addr)

            if result and result.get('values') and result['values']:
                # 获取单元格值
                cell_value = result['values'][0][0] if result['values'][0] else None
                if cell_value and str(cell_value).strip():
                    # 有数据，继续下一行
                    if row % 100 == 0:  # 每100行打印一次进度
                        print(f"  已检查到第{row}行，继续查找...")
                    continue
                else:
                    # 找到空行
                    print(f"✅ 找到空行: 第{row}行")
                    return row
            else:
                # 没有返回值或values为空，也视为空行
                print(f"✅ 找到空行: 第{row}行")
                return row

        print(f"⚠️ 在{max_rows}行内没有找到空行")
        return None

    def insert_data_at_empty_row(self, workbook_id: str, sheet_id: str, operator_id: str,
                                 data: List[Any], start_row: int = 1,
                                 start_col: str = 'A', auto_find: bool = True) -> Optional[int]:
        """
        在空行插入数据

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            data: 要插入的数据，一维列表 [col1, col2, col3, ...]
            start_row: 开始查找的行（当auto_find=False时，直接在此行插入）
            start_col: 起始列，默认'A'
            auto_find: 是否自动查找空行，True则查找第一个空行，False则使用start_row

        Returns:
            插入数据的行号，失败返回None
        """
        try:
            # 1. 确定要插入的行
            target_row = start_row
            if auto_find:
                empty_row = self.find_first_empty_row_optimized(
        workbook_id, sheet_id, operator_id
    )
                if not empty_row:
                    print("❌ 未找到空行，无法插入数据")
                    return None
                target_row = empty_row

            # 2. 确定数据范围
            col_count = len(data)
            if col_count == 0:
                print("❌ 没有数据需要插入")
                return None

            end_col = chr(ord(start_col) + col_count - 1)
            range_addr = f"{start_col}{target_row}:{end_col}{target_row}"

            print(f"\n📝 准备插入数据:")
            print(f"  目标行: {target_row}")
            print(f"  数据范围: {range_addr}")
            print(f"  数据内容: {data}")

            # 3. 准备数据格式（转换为二维列表）
            values = [data]

            # 4. 插入数据
            result = self.update_range_data(workbook_id, sheet_id, operator_id, range_addr, values)

            if result:
                print(f"✅ 数据插入成功！行号: {target_row}")
                return target_row
            else:
                print(f"❌ 数据插入失败")
                return None

        except Exception as e:
            print(f"❌ 插入数据异常: {e}")
            return None

    def insert_multiple_rows_at_empty(self, workbook_id: str, sheet_id: str, operator_id: str,
                                      rows_data: List[List[Any]], start_row: int = 1,
                                      start_col: str = 'A') -> Optional[int]:
        """
        插入多行数据到连续的空行

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            rows_data: 多行数据列表 [[row1_col1, row1_col2], [row2_col1, row2_col2]]
            start_row: 开始查找的行
            start_col: 起始列，默认'A'

        Returns:
            开始插入的行号，失败返回None
        """
        try:
            if not rows_data:
                print("❌ 没有数据需要插入")
                return None

            # 1. 找到第一个空行
            first_empty_row = self.find_first_empty_row(workbook_id, sheet_id, operator_id, start_row)
            if not first_empty_row:
                print("❌ 未找到空行，无法插入数据")
                return None

            # 2. 计算数据范围
            col_count = max(len(row) for row in rows_data)
            end_row = first_empty_row + len(rows_data) - 1
            end_col = chr(ord(start_col) + col_count - 1)
            range_addr = f"{start_col}{first_empty_row}:{end_col}{end_row}"

            print(f"\n📝 准备插入多行数据:")
            print(f"  开始行: {first_empty_row}")
            print(f"  结束行: {end_row}")
            print(f"  数据范围: {range_addr}")
            print(f"  数据行数: {len(rows_data)}")

            # 3. 插入数据
            result = self.update_range_data(workbook_id, sheet_id, operator_id, range_addr, rows_data)

            if result:
                print(f"✅ 多行数据插入成功！范围: {range_addr}")
                return first_empty_row
            else:
                print(f"❌ 多行数据插入失败")
                return None

        except Exception as e:
            print(f"❌ 插入多行数据异常: {e}")
            return None

    def insert_data_with_style(self, workbook_id: str, sheet_id: str, operator_id: str,
                               data: List[Any], background_color: Optional[str] = None,
                               number_format: str = '@', start_row: int = 1) -> Optional[int]:
        """
        带样式插入数据

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            data: 要插入的数据
            background_color: 背景颜色，如 '#FFFF00'
            number_format: 数字格式，'@'表示文本格式
            start_row: 开始查找的行

        Returns:
            插入数据的行号，失败返回None
        """
        try:
            # 1. 找到空行
            empty_row = self.find_first_empty_row(workbook_id, sheet_id, operator_id, start_row)
            if not empty_row:
                print("❌ 未找到空行")
                return None

            # 2. 确定数据范围
            col_count = len(data)
            end_col = chr(ord('A') + col_count - 1)
            range_addr = f"A{empty_row}:{end_col}{empty_row}"

            # 3. 构建带样式的请求
            url = f"{self.base_url}/v1.0/doc/workbooks/{workbook_id}/sheets/{sheet_id}/ranges/{range_addr}"

            body = {
                "operatorId": operator_id,
                "values": [data]
            }

            # 添加样式参数
            if background_color:
                body["backgroundColors"] = [[background_color] * col_count]
            if number_format:
                body["numberFormat"] = number_format

            print(f"\n🎨 准备插入带样式数据:")
            print(f"  行号: {empty_row}")
            print(f"  背景色: {background_color if background_color else '默认'}")
            print(f"  数据: {data}")

            result = self._make_request('PUT', url, json=body)

            if result:
                print(f"✅ 带样式的数据插入成功！行号: {empty_row}")
                return empty_row
            else:
                print(f"❌ 带样式的数据插入失败")
                return None

        except Exception as e:
            print(f"❌ 插入带样式数据异常: {e}")
            return None

    def batch_insert_from_dict(self, workbook_id: str, sheet_id: str, operator_id: str,
                               data_dict: Dict[str, List[Any]],
                               column_mapping: Optional[Dict[str, str]] = None) -> bool:
        """
        从字典批量插入数据（适用于不同列的数据）

        Args:
            workbook_id: 表格文档ID
            sheet_id: 工作表ID
            operator_id: 操作者ID
            data_dict: 数据字典，key为列名，value为该列的数据列表
            column_mapping: 列名到实际列的映射，如 {'姓名': 'A', '年龄': 'B'}

        Returns:
            是否全部插入成功
        """
        try:
            if not data_dict:
                print("❌ 没有数据需要插入")
                return False

            # 如果没有提供映射，尝试自动映射（A, B, C...）
            if not column_mapping:
                columns = list(data_dict.keys())
                column_mapping = {}
                for i, col_name in enumerate(columns):
                    column_mapping[col_name] = chr(ord('A') + i)

            print(f"\n📊 开始批量插入数据:")
            print(f"  列映射: {column_mapping}")

            # 获取所有列的最大行数
            max_rows = max(len(values) for values in data_dict.values())

            # 找到第一个空行
            start_row = self.find_first_empty_row(workbook_id, sheet_id, operator_id)
            if not start_row:
                print("❌ 未找到空行")
                return False

            success_count = 0
            # 按列插入数据
            for col_name, values in data_dict.items():
                col_letter = column_mapping.get(col_name)
                if not col_letter:
                    print(f"⚠️ 跳过列 {col_name}，未找到映射")
                    continue

                # 为这一列准备数据（按行组织）
                for i, value in enumerate(values):
                    if i >= max_rows:
                        break

                    target_row = start_row + i
                    range_addr = f"{col_letter}{target_row}"

                    # 插入单个单元格数据
                    result = self.update_range_data(workbook_id, sheet_id, operator_id,
                                                    range_addr, [[value]])

                    if result:
                        success_count += 1
                        print(f"  ✅ 已插入: {col_name}{target_row} = {value}")
                    else:
                        print(f"  ❌ 插入失败: {col_name}{target_row} = {value}")

                    time.sleep(0.1)  # 避免请求过快

            print(f"\n✅ 批量插入完成，成功插入 {success_count} 个单元格")
            return True

        except Exception as e:
            print(f"❌ 批量插入异常: {e}")
            return False




def main():
    """
    主函数 - 演示智能插入功能
    """
    # 初始化
    token_manager = DingTalkTokenManager()

    # 创建客户端实例
    client = DingTalkDocClient(token_manager)

    # 设置参数
    workbook_id = "kDnRL6jAJMO3D450HBM0ogPDWyMoPYe1"
    operator_id = "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    sheet_id = "st-514b97fa-74330"

    # 获取所有工作表
    result = client.get_all_sheets(workbook_id, operator_id)

    # 打印结果
    if result:
        print("\n📊 获取到的工作表信息:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("\n❌ 获取工作表失败")

    # ==================== 演示新增的空行插入功能 ====================

    print("\n" + "=" * 50)
    print("🚀 开始演示空行插入功能")
    print("=" * 50)

    # 示例1：查找第一个空行
    # 方法1：二分查找法（最快）
    empty_row = client.find_first_empty_row_optimized(
        workbook_id, sheet_id, operator_id
    )

    if empty_row:
        print(f"找到空行: {empty_row}")

    # 示例2：在空行插入单行数据
    single_row_data = ["张三", "25", "北京", "工程师"]
    inserted_row = client.insert_data_at_empty_row(
        workbook_id,
        sheet_id,
        operator_id,
        single_row_data
    )

    # 等待一下，避免请求过快
    time.sleep(1)

    # 示例3：插入多行数据
    multiple_rows_data = [
        ["李四", "28", "上海", "设计师"],
        ["王五", "30", "广州", "经理"],
        ["赵六", "26", "深圳", "产品经理"]
    ]
    start_row = client.insert_multiple_rows_at_empty(
        workbook_id,
        sheet_id,
        operator_id,
        multiple_rows_data
    )

    # 等待一下，避免请求过快
    time.sleep(1)

    # 示例4：从指定行开始查找空行
    # custom_start_data = ["自定义行数据", "测试", "从第5行开始找"]
    # inserted_row = client.insert_data_at_empty_row(
    #     workbook_id,
    #     sheet_id,
    #     operator_id,
    #     custom_start_data,
    #     start_row=5  # 从第5行开始找空行
    # )
    #
    # # 等待一下，避免请求过快
    # time.sleep(1)

    # 示例5：带样式插入数据
    # style_data = ["标题1", "标题2", "标题3"]
    # inserted_row = client.insert_data_with_style(
    #     workbook_id,
    #     sheet_id,
    #     operator_id,
    #     style_data,
    #     background_color="#FFFF00",  # 黄色背景
    #     number_format="@"
    # )

    # 等待一下，避免请求过快
    time.sleep(1)

    # 示例6：批量从字典插入
    # batch_data = {
    #     "姓名": ["小明", "小红", "小刚"],
    #     "年龄": ["18", "19", "20"],
    #     "城市": ["北京", "上海", "广州"]
    # }
    # success = client.batch_insert_from_dict(
    #     workbook_id,
    #     sheet_id,
    #     operator_id,
    #     batch_data
    # )

    print("\n" + "=" * 50)
    print("🏁 演示完成")
    print("=" * 50)


if __name__ == "__main__":
    main()