
import json
import os
from datetime import datetime
import requests
from typing import Dict, Any, List, Optional
import time


class DingTalkTokenManager:
    """钉钉Token管理器"""

    def __init__(self, config_path: str = "./data/config.json", token_cache_path: str = "./data/token_cache.json"):
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


class DingTalkSheetUploader:
    def __init__(self, base_id: str, sheet_id: str, operator_id: str,token_manager: DingTalkTokenManager = None):
        """
        初始化钉钉表格上传器

        Args:
            base_id: 多维表ID
            sheet_id: 工作表名称
            operator_id: 操作人ID
            token_manager: Token管理器实例
        """
        self.base_id = base_id
        self.sheet_id = sheet_id
        self.operator_id = operator_id

        # 初始化token管理器
        if token_manager is None:
            self.token_manager = DingTalkTokenManager()
        else:
            self.token_manager = token_manager

        # 获取初始token
        self.access_token = self.token_manager.get_access_token()

        # 钉钉API地址
        self.url = f"https://api.dingtalk.com/v1.0/notable/bases/{base_id}/sheets/{sheet_id}/records"

        # 请求头（会在每次请求时更新）
        self.headers = self._get_headers()

        # 请求参数
        self.params = {
            "operatorId": operator_id
        }

    def _get_headers(self) -> Dict[str, str]:
        """获取请求头，包含当前token"""
        return {
            "Content-Type": "application/json",
            "x-acs-dingtalk-access-token": self.access_token if self.access_token else ""
        }

    def _refresh_token_if_needed(self) -> bool:
        """检查并刷新token，如果需要"""
        if not self.access_token:
            print("token为空，尝试刷新...")
            self.access_token = self.token_manager.get_access_token(force_refresh=True)
            if self.access_token:
                self.headers = self._get_headers()
                return True
            return False
        return True

    def upload_batch_records(self, records_data: List[Dict[str, Any]], batch_size: int = 100, delay: float = 0.1,max_retries: int = 2) -> List[Dict[str, Any]]:
        """
        批量上传多条记录
        Args:
            records_data: 多条记录数据列表
            batch_size: 每次批量上传的记录数（钉钉API可能有单次请求数量限制）
            delay: 批次间的延迟时间（秒），避免请求频率过高
            max_retries: 失败时的最大重试次数

        Returns:
            所有批次的上传结果列表
        """
        results = []

        # 确保有有效的token
        if not self._refresh_token_if_needed():
            print("无法获取有效token，终止上传")
            return [{
                "success": False,
                "message": "无法获取有效token",
                "total_records": len(records_data)
            }]

        # 将数据分成多个批次
        for i in range(0, len(records_data), batch_size):
            batch = records_data[i:i + batch_size]

            # 尝试上传批次，支持重试
            batch_result = self._upload_batch_with_retry(batch, max_retries)
            results.append(batch_result)

            # 添加延迟，避免请求过于频繁
            if i + batch_size < len(records_data):
                time.sleep(delay)

            # 打印进度
            print(f"已上传 {min(i + batch_size, len(records_data))}/{len(records_data)} 条记录")

        return results

    def _upload_batch_with_retry(self, batch_data: List[Dict[str, Any]],max_retries: int) -> Dict[str, Any]:
        """
        带重试机制的上传批次数据

        Args:
            batch_data: 批次数据
            max_retries: 最大重试次数

        Returns:
            上传结果
        """
        for retry in range(max_retries + 1):
            result = self._upload_batch(batch_data)

            # 如果成功或者不是token相关错误，直接返回
            if result["success"]:
                return result

            # 检查是否是token过期或无效的错误
            error_message = str(result.get("message", ""))
            if retry < max_retries and any(keyword in error_message.lower() for keyword in
                                           ["token", "unauthorized", "auth", "401", "403"]):
                print(f"检测到token相关错误，尝试刷新token并重试 (第{retry + 1}次重试)")

                # 刷新token
                self.access_token = self.token_manager.get_access_token(force_refresh=True)
                if self.access_token:
                    self.headers = self._get_headers()
                    print("token刷新成功，重新尝试上传")
                    time.sleep(1)  # 等待一下再重试
                else:
                    print("token刷新失败")
                    break
            else:
                # 不是token错误或者已达最大重试次数
                break

        return result

    def _upload_batch(self, batch_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        上传一个批次的数据

        Args:
            batch_data: 一个批次的记录数据

        Returns:
            批次上传结果
        """
        data = {
            "records": [
                {"fields": record} for record in batch_data
            ]
        }

        try:
            response = requests.post(
                url=self.url,
                headers=self.headers,
                params=self.params,
                json=data,
                timeout=30
            )

            response.raise_for_status()
            return {
                "success": True,
                "status_code": response.status_code,
                "data": response.json() if response.status_code == 200 else None,
                "message": f"批次上传成功，共 {len(batch_data)} 条记录",
                "batch_size": len(batch_data)
            }

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            error_msg = f"HTTP错误 {status_code}: {str(e)}"
            return {
                "success": False,
                "status_code": status_code,
                "data": None,
                "message": error_msg,
                "batch_size": len(batch_data)
            }
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "status_code": None,
                "data": None,
                "message": f"请求异常: {str(e)}",
                "batch_size": len(batch_data)
            }
        except Exception as e:
            return {
                "success": False,
                "status_code": None,
                "data": None,
                "message": f"其他异常: {str(e)}",
                "batch_size": len(batch_data)
            }


class DingTalkSheetQuery:
    def __init__(self, base_id: str, sheet_id: str, operator_id: str, token_manager: DingTalkTokenManager = None):
        """
        初始化钉钉表格查询器

        Args:
            base_id: 多维表ID
            sheet_id: 工作表名称或ID
            operator_id: 操作人ID
            token_manager: Token管理器实例
        """
        self.base_id = base_id
        self.sheet_id = sheet_id
        self.operator_id = operator_id

        # 初始化token管理器
        if token_manager is None:
            self.token_manager = DingTalkTokenManager()
        else:
            self.token_manager = token_manager

        # 获取初始token
        self.access_token = self.token_manager.get_access_token()

        # 基础API地址
        self.base_url = f"https://api.dingtalk.com/v1.0/notable/bases/{base_id}/sheets/{sheet_id}"

        # 请求头
        self.headers = self._get_headers()

        # 请求参数
        self.params = {
            "operatorId": operator_id
        }

    def _get_headers(self) -> Dict[str, str]:
        """获取请求头，包含当前token"""
        return {
            "Content-Type": "application/json",
            "x-acs-dingtalk-access-token": self.access_token if self.access_token else ""
        }

    def _refresh_token_if_needed(self) -> bool:
        """检查并刷新token，如果需要"""
        if not self.access_token:
            print("token为空，尝试刷新...")
            self.access_token = self.token_manager.get_access_token(force_refresh=True)
            if self.access_token:
                self.headers = self._get_headers()
                return True
            return False
        return True

    def _make_request_with_token_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        带token重试机制的请求方法

        Args:
            method: HTTP方法
            url: 请求URL
            **kwargs: 其他请求参数

        Returns:
            requests.Response对象
        """
        max_retries = 2
        for attempt in range(max_retries + 1):
            # 确保有有效的token
            if not self._refresh_token_if_needed():
                raise Exception("无法获取有效token")

            # 更新headers中的token
            self.headers = self._get_headers()
            kwargs['headers'] = self.headers

            try:
                response = requests.request(method, url, **kwargs)

                # 检查是否是token过期或无效的错误
                if response.status_code in [401, 403] and attempt < max_retries:
                    error_data = response.json() if response.text else {}
                    error_msg = error_data.get('message', '').lower() if isinstance(error_data, dict) else ''

                    # 尝试刷新token并重试
                    print(f"检测到授权错误，尝试刷新token (第{attempt + 1}次重试)")
                    self.access_token = self.token_manager.get_access_token(force_refresh=True)
                    time.sleep(1)
                    continue

                return response

            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    raise e
                print(f"请求异常，重试中... (第{attempt + 1}次)")
                time.sleep(1)

    def get_record_by_id(self, record_id: str) -> Dict[str, Any]:
        """
        根据记录ID查询单条记录

        Args:
            record_id: 记录ID

        Returns:
            包含记录信息的字典
        """
        url = f"{self.base_url}/records/{record_id}"

        try:
            response = self._make_request_with_token_retry(
                method="GET",
                url=url,
                params=self.params,
                timeout=30
            )

            response.raise_for_status()
            return {
                "success": True,
                "status_code": response.status_code,
                "data": response.json(),
                "message": "查询成功"
            }

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            error_msg = f"HTTP错误 {status_code}: {str(e)}"
            return {
                "success": False,
                "status_code": status_code,
                "data": None,
                "message": error_msg
            }
        except Exception as e:
            return {
                "success": False,
                "status_code": None,
                "data": None,
                "message": f"查询异常: {str(e)}"
            }

    def query_records(self,
                      filter: Optional[str] = None,
                      sort: Optional[List[Dict[str, str]]] = None,
                      field_names: Optional[List[str]] = None,
                      max_results: int = 100,
                      next_token: Optional[str] = None) -> Dict[str, Any]:
        """
        查询多条记录（支持分页、过滤、排序）

        Args:
            filter: 过滤条件，例如："platform='速卖通' and today_sales>10"
            sort: 排序规则，例如：[{"field": "today_sales", "order": "desc"}]
            field_names: 要返回的字段列表
            max_results: 每页最大记录数
            next_token: 下一页的token（用于分页）

        Returns:
            查询结果
        """
        url = f"{self.base_url}/records"

        # 构建请求参数
        params = self.params.copy()
        if filter:
            params["filter"] = filter
        if sort:
            params["sort"] = json.dumps(sort) if isinstance(sort, list) else sort
        if field_names:
            params["fieldNames"] = json.dumps(field_names)
        if next_token:
            params["nextToken"] = next_token

        params["maxResults"] = max_results

        try:
            response = self._make_request_with_token_retry(
                method="GET",
                url=url,
                params=params,
                timeout=30
            )

            response.raise_for_status()
            result = response.json()

            return {
                "success": True,
                "status_code": response.status_code,
                "data": result.get("records", []),
                "next_token": result.get("nextToken"),
                "total": len(result.get("records", [])),
                "message": f"查询成功，共获取 {len(result.get('records', []))} 条记录"
            }

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            error_msg = f"HTTP错误 {status_code}: {str(e)}"
            return {
                "success": False,
                "status_code": status_code,
                "data": None,
                "next_token": None,
                "total": 0,
                "message": error_msg
            }
        except Exception as e:
            return {
                "success": False,
                "status_code": None,
                "data": None,
                "next_token": None,
                "total": 0,
                "message": f"查询异常: {str(e)}"
            }

    def get_all_records(self,
                        filter: Optional[str] = None,
                        sort: Optional[List[Dict[str, str]]] = None,
                        field_names: Optional[List[str]] = None,
                        batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        获取所有记录（自动处理分页）

        Args:
            filter: 过滤条件
            sort: 排序规则
            field_names: 要返回的字段列表
            batch_size: 每批次获取的记录数

        Returns:
            所有记录的列表
        """
        all_records = []
        next_token = None
        page = 1

        while True:
            print(f"正在获取第 {page} 页数据...")

            result = self.query_records(
                filter=filter,
                sort=sort,
                field_names=field_names,
                max_results=batch_size,
                next_token=next_token
            )

            if result["success"]:
                records = result["data"]
                if records:
                    all_records.extend(records)
                    print(f"  获取到 {len(records)} 条记录")

                next_token = result.get("next_token")
                if not next_token:
                    print(f"所有数据获取完成，共 {len(all_records)} 条记录")
                    break

                page += 1
                time.sleep(0.5)  # 避免请求过于频繁
            else:
                print(f"查询失败: {result['message']}")
                break

        return all_records


class DingTalkSheetDeleter:
    def __init__(self, base_id: str, sheet_id: str, operator_id: str, token_manager: DingTalkTokenManager = None):
        """
        初始化钉钉表格删除器

        Args:
            base_id: 多维表ID
            sheet_id: 工作表名称或ID
            operator_id: 操作人ID
            token_manager: Token管理器实例
        """
        self.base_id = base_id
        self.sheet_id = sheet_id
        self.operator_id = operator_id

        # 初始化token管理器
        if token_manager is None:
            self.token_manager = DingTalkTokenManager()
        else:
            self.token_manager = token_manager

        # 获取初始token
        self.access_token = self.token_manager.get_access_token()

        # 删除API地址
        self.delete_url = f"https://api.dingtalk.com/v1.0/notable/bases/{base_id}/sheets/{sheet_id}/records/delete"

        # 请求头
        self.headers = self._get_headers()

        # 请求参数
        self.params = {
            "operatorId": operator_id
        }

    def _get_headers(self) -> Dict[str, str]:
        """获取请求头，包含当前token"""
        return {
            "Content-Type": "application/json",
            "x-acs-dingtalk-access-token": self.access_token if self.access_token else ""
        }

    def _refresh_token_if_needed(self) -> bool:
        """检查并刷新token，如果需要"""
        if not self.access_token:
            print("token为空，尝试刷新...")
            self.access_token = self.token_manager.get_access_token(force_refresh=True)
            if self.access_token:
                self.headers = self._get_headers()
                return True
            return False
        return True

    def _make_request_with_token_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        带token重试机制的请求方法

        Args:
            method: HTTP方法
            url: 请求URL
            **kwargs: 其他请求参数

        Returns:
            requests.Response对象
        """
        max_retries = 2
        for attempt in range(max_retries + 1):
            # 确保有有效的token
            if not self._refresh_token_if_needed():
                raise Exception("无法获取有效token")

            # 更新headers中的token
            self.headers = self._get_headers()
            if 'headers' not in kwargs:
                kwargs['headers'] = self.headers
            else:
                kwargs['headers']['x-acs-dingtalk-access-token'] = self.access_token

            try:
                response = requests.request(method, url, **kwargs)

                # 检查是否是token过期或无效的错误
                if response.status_code in [401, 403] and attempt < max_retries:
                    error_data = response.json() if response.text else {}
                    error_msg = error_data.get('message', '').lower() if isinstance(error_data, dict) else ''

                    # 尝试刷新token并重试
                    print(f"检测到授权错误，尝试刷新token (第{attempt + 1}次重试)")
                    self.access_token = self.token_manager.get_access_token(force_refresh=True)
                    time.sleep(1)
                    continue

                return response

            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    raise e
                print(f"请求异常，重试中... (第{attempt + 1}次)")
                time.sleep(1)

    def delete_records_by_ids(self, record_ids: List[str], batch_size: int = 100, delay: float = 0.1) -> List[
        Dict[str, Any]]:
        """
        根据记录ID批量删除记录

        Args:
            record_ids: 记录ID列表
            batch_size: 每批次删除的记录数（钉钉API可能有单次请求数量限制）
            delay: 批次间的延迟时间（秒），避免请求频率过高

        Returns:
            删除结果列表
        """
        results = []

        # 确保有有效的token
        if not self._refresh_token_if_needed():
            print("无法获取有效token，终止删除操作")
            return [{
                "success": False,
                "message": "无法获取有效token",
                "total_records": len(record_ids)
            }]

        # 将记录ID分成多个批次
        for i in range(0, len(record_ids), batch_size):
            batch = record_ids[i:i + batch_size]

            # 删除当前批次
            batch_result = self._delete_batch(batch)
            results.append(batch_result)

            # 添加延迟，避免请求过于频繁
            if i + batch_size < len(record_ids):
                time.sleep(delay)

            # 打印进度
            print(f"已删除 {min(i + batch_size, len(record_ids))}/{len(record_ids)} 条记录")

        return results

    def _delete_batch(self, record_ids: List[str]) -> Dict[str, Any]:
        """
        删除一个批次的记录

        Args:
            record_ids: 记录ID列表

        Returns:
            删除结果
        """
        data = {
            "recordIds": record_ids
        }

        try:
            response = self._make_request_with_token_retry(
                method="POST",
                url=self.delete_url,
                params=self.params,
                json=data,
                timeout=30
            )

            response.raise_for_status()
            return {
                "success": True,
                "status_code": response.status_code,
                "data": response.json() if response.text else None,
                "message": f"成功删除 {len(record_ids)} 条记录",
                "deleted_count": len(record_ids)
            }

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            error_msg = f"HTTP错误 {status_code}: {str(e)}"
            return {
                "success": False,
                "status_code": status_code,
                "data": None,
                "message": error_msg,
                "deleted_count": 0
            }
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "status_code": None,
                "data": None,
                "message": f"请求异常: {str(e)}",
                "deleted_count": 0
            }
        except Exception as e:
            return {
                "success": False,
                "status_code": None,
                "data": None,
                "message": f"其他异常: {str(e)}",
                "deleted_count": 0
            }

    def delete_all_records(self, batch_size: int = 100, delay: float = 0.1, confirm: bool = False) -> Dict[str, Any]:
        """
        删除工作表中的所有记录（谨慎使用！）

        Args:
            batch_size: 每批次删除的记录数
            delay: 批次间的延迟时间
            confirm: 是否确认删除所有记录，为True时才执行删除

        Returns:
            删除结果汇总
        """
        if not confirm:
            print("⚠️ 警告：此操作将删除工作表中的所有记录！")
            print("如果要继续，请将参数 confirm 设置为 True")
            return {
                "success": False,
                "message": "未确认删除操作",
                "total_deleted": 0
            }

        print("正在获取所有记录ID...")

        # 首先查询所有记录ID
        query = DingTalkSheetQuery(
            base_id=self.base_id,
            sheet_id=self.sheet_id,
            operator_id=self.operator_id,
            token_manager=self.token_manager
        )

        # 获取所有记录
        all_records = query.get_all_records(batch_size=batch_size)

        if not all_records:
            print("工作表为空，无需删除")
            return {
                "success": True,
                "message": "工作表为空，无需删除",
                "total_deleted": 0
            }

        # 提取所有记录ID（假设记录都有_id字段）
        record_ids = []
        for record in all_records:
            # 注意：钉钉API返回的记录结构可能有所不同，需要根据实际情况调整
            if 'recordId' in record:
                record_ids.append(record['recordId'])
            elif 'id' in record:
                record_ids.append(record['id'])
            elif 'fields' in record and 'recordId' in record['fields']:
                record_ids.append(record['fields']['recordId'])

        if not record_ids:
            print("未能提取到有效的记录ID")
            return {
                "success": False,
                "message": "未能提取到有效的记录ID",
                "total_deleted": 0
            }

        print(f"找到 {len(record_ids)} 条记录，开始删除...")

        # 批量删除所有记录
        results = self.delete_records_by_ids(record_ids, batch_size=batch_size, delay=delay)

        # 统计结果
        successful_batches = [r for r in results if r.get("success")]
        failed_batches = [r for r in results if not r.get("success")]
        total_deleted = sum(r.get("deleted_count", 0) for r in successful_batches)

        summary = {
            "success": len(failed_batches) == 0,
            "total_records": len(record_ids),
            "total_deleted": total_deleted,
            "total_batches": len(results),
            "successful_batches": len(successful_batches),
            "failed_batches": len(failed_batches),
            "details": results
        }

        print(f"\n删除完成统计:")
        print(f"总记录数: {summary['total_records']}")
        print(f"成功删除: {summary['total_deleted']}")
        print(f"总批次: {summary['total_batches']}")
        print(f"成功批次: {summary['successful_batches']}")
        print(f"失败批次: {summary['failed_batches']}")

        return summary

    def delete_records_by_filter(self, filter_condition: str, batch_size: int = 100, delay: float = 0.1,
                                 confirm: bool = False) -> Dict[str, Any]:
        """
        根据过滤条件删除记录

        Args:
            filter_condition: 过滤条件，例如："platform='速卖通'"
            batch_size: 每批次删除的记录数
            delay: 批次间的延迟时间
            confirm: 是否确认删除，为True时才执行删除

        Returns:
            删除结果汇总
        """
        if not confirm:
            print(f"⚠️ 警告：此操作将删除所有符合条件 '{filter_condition}' 的记录！")
            print("如果要继续，请将参数 confirm 设置为 True")
            return {
                "success": False,
                "message": "未确认删除操作",
                "total_deleted": 0
            }

        print(f"正在查询符合条件 '{filter_condition}' 的记录...")

        # 首先查询符合条件的记录
        query = DingTalkSheetQuery(
            base_id=self.base_id,
            sheet_id=self.sheet_id,
            operator_id=self.operator_id,
            token_manager=self.token_manager
        )

        # 获取符合条件的记录
        all_records = query.get_all_records(filter=filter_condition, batch_size=batch_size)

        if not all_records:
            print(f"没有找到符合条件的记录")
            return {
                "success": True,
                "message": "没有找到符合条件的记录",
                "total_deleted": 0
            }

        # 提取记录ID
        record_ids = []
        for record in all_records:
            if 'recordId' in record:
                record_ids.append(record['recordId'])
            elif 'id' in record:
                record_ids.append(record['id'])
            elif 'fields' in record and 'recordId' in record['fields']:
                record_ids.append(record['fields']['recordId'])

        if not record_ids:
            print("未能提取到有效的记录ID")
            return {
                "success": False,
                "message": "未能提取到有效的记录ID",
                "total_deleted": 0
            }

        print(f"找到 {len(record_ids)} 条符合条件的记录，开始删除...")

        # 批量删除记录
        results = self.delete_records_by_ids(record_ids, batch_size=batch_size, delay=delay)

        # 统计结果
        successful_batches = [r for r in results if r.get("success")]
        failed_batches = [r for r in results if not r.get("success")]
        total_deleted = sum(r.get("deleted_count", 0) for r in successful_batches)

        summary = {
            "success": len(failed_batches) == 0,
            "filter": filter_condition,
            "total_matched": len(record_ids),
            "total_deleted": total_deleted,
            "total_batches": len(results),
            "successful_batches": len(successful_batches),
            "failed_batches": len(failed_batches),
            "details": results
        }

        print(f"\n删除完成统计:")
        print(f"过滤条件: {summary['filter']}")
        print(f"匹配记录: {summary['total_matched']}")
        print(f"成功删除: {summary['total_deleted']}")
        print(f"总批次: {summary['total_batches']}")
        print(f"成功批次: {summary['successful_batches']}")
        print(f"失败批次: {summary['failed_batches']}")

        return summary




# 示例使用函数
def upload_multiple_records():
    """
    批量上传多条记录的完整示例
    """
    # 配置参数（请替换为实际值）
    config = {
        "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
        "sheet_id": "销量与库存-日更",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建上传器（不再需要手动传入access_token）
    uploader = DingTalkSheetUploader(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    # 生成测试数据（实际使用中从数据库或文件读取）
    records = []
    for i in range(1, 101):  # 100条测试数据
        record = {
            "抓取数据日期": 1765188638050 + i,
            "近30天销量": str(30 + i),
            "商品名称": f"测试商品 {i}",
            "平台库存": str(i % 20),
            "平台": "速卖通" if i % 2 == 0 else "亚马逊",
            "在途库存": str(50 + i),
            "今日销量": str(i % 5),
            "sku": f"JX-{i:03d}",
            "近7天销量": str(5 + i % 10),
            "店铺": f"SMT{200 + i}" if i % 2 == 0 else f"AMZ{100 + i}"
        }
        records.append(record)

    print(f"准备上传 {len(records)} 条记录...")

    # 批量上传，每批50条，批次间延迟0.2秒，失败时重试2次
    results = uploader.upload_batch_records(records, batch_size=50, delay=0.2, max_retries=2)

    # 分析结果
    successful_batches = [r for r in results if r.get("success")]
    failed_batches = [r for r in results if not r.get("success")]

    print(f"\n上传统计:")
    print(f"总批次: {len(results)}")
    print(f"成功批次: {len(successful_batches)}")
    print(f"失败批次: {len(failed_batches)}")

    if failed_batches:
        print(f"\n失败详情:")
        for i, failed in enumerate(failed_batches):
            print(f"  批次 {i + 1}: {failed.get('message', '未知错误')}")

    return results

def test_token_manager():
    """测试Token管理器"""
    print("测试Token管理器...")
    token_manager = DingTalkTokenManager()

    # 测试获取token
    token = token_manager.get_access_token()
    if token:
        print(f"成功获取token: {token[:20]}...")
    else:
        print("获取token失败")

    # 测试强制刷新
    print("\n测试强制刷新...")
    token = token_manager.get_access_token(force_refresh=True)
    if token:
        print(f"强制刷新成功: {token[:20]}...")
    else:
        print("强制刷新失败")


def test_query_records():
    """测试查询功能"""
    # 配置参数
    config = {
        "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
        "sheet_id": "销量与库存-日更",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建查询器
    query = DingTalkSheetQuery(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )


    print("\n3. 测试范围查询")
    # 查询今日销量大于5的记录
    result = query.query_records(filter="今日销量>5", max_results=10)
    if result["success"]:
        print(f"找到 {result['total']} 条记录")

    print("\n4. 测试获取所有记录（分页）")
    # 获取所有记录，每次获取50条
    all_records = query.get_all_records(batch_size=50)
    # print(all_records)
    print(f"工作表总共有 {len(all_records)} 条记录")


def test_delete_records():
    """测试删除功能"""
    # 配置参数
    config = {
        "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
        "sheet_id": "销量与库存-日更",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建删除器
    deleter = DingTalkSheetDeleter(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )


    print("\n2. 测试根据条件删除记录")
    # 删除所有平台为"测试"的记录（请根据实际情况调整条件）
    # 注意：这里使用confirm=False，不会实际执行删除
    filter_result = deleter.delete_records_by_filter(
        filter_condition="平台='SHEIN'",
        batch_size=50,
        delay=0.2,
        confirm=True  # 设置为True才会实际删除
    )
    print(f"条件删除结果: {filter_result.get('message')}")

    print("\n3. 测试删除所有记录")
    # 删除所有记录（谨慎使用！）
    # 注意：这里使用confirm=False，不会实际执行删除
    delete_all_result = deleter.delete_all_records(
        batch_size=50,
        delay=0.2,
        confirm=False  # 设置为True才会实际删除
    )
    print(f"删除所有记录结果: {delete_all_result.get('message')}")

    return deleter


if __name__ == "__main__":
    # 测试Token管理器
    test_token_manager()

    # 或者运行批量上传示例
    # upload_multiple_records()

    # 运行查询测试
    # query_instance = test_query_records()

    #运行删除测试
    delete_instance = test_delete_records()
