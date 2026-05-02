
import json
import os
from datetime import datetime
import requests
from typing import Dict, Any, List, Optional
import time
from pathlib import Path

"""上传/删除/查询钉钉多维表的数据"""

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "config.json"
token_cache=Path(__file__).resolve().parent.parent.parent / "token_cache.json"


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



class DingTalkSheetUploader:
    def __init__(self, base_id: str, sheet_id: str, operator_id: str,
                 token_manager: DingTalkTokenManager = None):
        self.base_id = base_id
        self.sheet_id = sheet_id
        self.operator_id = operator_id

        self.token_manager = token_manager or DingTalkTokenManager()
        self.access_token = self.token_manager.get_access_token()

        self.url = f"https://api.dingtalk.com/v1.0/notable/bases/{base_id}/sheets/{sheet_id}/records"

        self.headers = self._get_headers()
        self.params = {"operatorId": operator_id}

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "x-acs-dingtalk-access-token": self.access_token or ""
        }

    def _refresh_token_if_needed(self) -> bool:
        if not self.access_token:
            self.access_token = self.token_manager.get_access_token(force_refresh=True)
            if not self.access_token:
                return False
            self.headers = self._get_headers()
        return True

    # ================= 新增：HTTP 错误翻译 =================
    def _parse_http_error(self, response: requests.Response) -> str:
        try:
            data = response.json()
        except Exception:
            data = {}

        code = data.get("code")
        message = data.get("message", "")
        status = response.status_code

        if status == 404:
            return (
                "❌ 钉钉多维表资源不存在（404）\n"
                "👉 常见原因：\n"
                "  1️⃣ sheet_id 不属于该 base_id\n"
                "  2️⃣ sheet 已被删除 / 复制后 ID 变化\n"
                f"BaseId: {self.base_id}\n"
                f"SheetId: {self.sheet_id}\n"
                f"钉钉返回: {message or code}"
            )

        if status == 401:
            return (
                "❌ 鉴权失败（401）\n"
                "👉 access_token 失效或错误\n"
                f"钉钉返回: {message or code}"
            )

        if status == 403:
            return (
                "❌ 无权限操作（403）\n"
                "👉 operator_id 无该多维表权限\n"
                f"OperatorId: {self.operator_id}\n"
                f"钉钉返回: {message or code}"
            )

        return f"❌ HTTP错误 {status}: {message or code}"

    # ================= 批量上传 =================
    def upload_batch_records(
            self,
            records_data: List[Dict[str, Any]],
            batch_size: int = 100,
            delay: float = 0.1,
            max_retries: int = 3,
            retry_failed: bool = True  # 新增：是否重试失败的批次
    ) -> Dict[str, Any]:
        """
        批量上传记录，支持失败重试和失败批次收集

        Args:
            records_data: 要上传的记录数据列表
            batch_size: 每批上传的记录数
            delay: 批次间延迟（秒）
            max_retries: 每批次最大重试次数
            retry_failed: 是否在上传完成后重新尝试失败的批次

        Returns:
            包含上传结果的字典，包括成功和失败的记录
        """
        results = {
            "success": True,
            "total_records": len(records_data),
            "successful_records": [],
            "failed_batches": [],
            "failed_records": [],
            "batch_results": []
        }

        if not self._refresh_token_if_needed():
            results["success"] = False
            results["message"] = "无法获取有效 token"
            return results

        # 分批上传
        for i in range(0, len(records_data), batch_size):
            batch = records_data[i:i + batch_size]
            batch_result = self._upload_batch_with_retry(batch, max_retries)

            # 记录批次结果
            results["batch_results"].append(batch_result)

            if batch_result["success"]:
                # 成功批次：记录成功的记录
                results["successful_records"].extend(batch)
                print(f"✅ 批次 {i // batch_size + 1} 上传成功，{len(batch)} 条记录")
            else:
                # 失败批次：记录失败的批次和记录
                results["failed_batches"].append({
                    "batch_index": i // batch_size + 1,
                    "records": batch,
                    "error": batch_result.get("message", "未知错误")
                })
                results["failed_records"].extend(batch)
                print(f"❌ 批次 {i // batch_size + 1} 上传失败: {batch_result.get('message')}")

            print(f"进度: {min(i + batch_size, len(records_data))}/{len(records_data)} 条记录")

            if i + batch_size < len(records_data):
                time.sleep(delay)

        # 如果有失败的批次且需要重试
        if retry_failed and results["failed_batches"]:
            print(f"\n🔄 发现 {len(results['failed_batches'])} 个失败批次，开始重试...")
            retry_results = self._retry_failed_batches(results["failed_batches"], max_retries)

            # 更新结果
            results["retry_results"] = retry_results
            results["successful_records"].extend(retry_results["successful_records"])
            results["failed_batches"] = retry_results["failed_batches"]
            results["failed_records"] = [r for batch in results["failed_batches"] for r in batch["records"]]

        # 更新最终状态
        results["success"] = len(results["failed_batches"]) == 0
        results["success_count"] = len(results["successful_records"])
        results["failed_count"] = len(results["failed_records"])
        results["message"] = f"上传完成: 成功 {results['success_count']} 条, 失败 {results['failed_count']} 条"

        return results

    def _retry_failed_batches(self, failed_batches: List[Dict], max_retries: int) -> Dict[str, Any]:
        """
        重试失败的批次

        Args:
            failed_batches: 失败的批次列表
            max_retries: 最大重试次数

        Returns:
            重试结果
        """
        retry_results = {
            "successful_records": [],
            "failed_batches": []
        }

        for batch_info in failed_batches:
            batch = batch_info["records"]
            print(f"重试批次 {batch_info['batch_index']} ({len(batch)} 条记录)...")

            # 重试上传
            batch_result = self._upload_batch_with_retry(batch, max_retries)

            if batch_result["success"]:
                print(f"  ✅ 重试成功")
                retry_results["successful_records"].extend(batch)
            else:
                print(f"  ❌ 重试失败: {batch_result.get('message')}")
                retry_results["failed_batches"].append(batch_info)

            time.sleep(1)  # 重试批次间延迟

        return retry_results

    def _upload_batch_with_retry(self, batch_data, max_retries):
        for retry in range(max_retries + 1):
            result = self._upload_batch(batch_data)

            if result["success"]:
                return result

            msg = str(result.get("message", "")).lower()
            if retry < max_retries and any(k in msg for k in ["401", "403", "token", "auth"]):
                self.access_token = self.token_manager.get_access_token(force_refresh=True)
                if self.access_token:
                    self.headers = self._get_headers()
                    time.sleep(1)
                else:
                    break
            else:
                break

        return result

    # ================= 核心上传 =================
    def _upload_batch(self, batch_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload = {
            "records": [{"fields": record} for record in batch_data]
        }

        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                params=self.params,
                json=payload,
                timeout=30
            )

            if not response.ok:
                error_msg = self._parse_http_error(response)
                return {
                    "success": False,
                    "status_code": response.status_code,
                    "message": error_msg,
                    "batch_size": len(batch_data)
                }

            return {
                "success": True,
                "status_code": response.status_code,
                "data": response.json(),
                "message": f"批次上传成功，共 {len(batch_data)} 条记录",
                "batch_size": len(batch_data)
            }

        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "status_code": None,
                "message": f"请求异常: {str(e)}",
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

class DingTalkSheetManager:
    """
    钉钉多维表 Sheet 管理器
    - 查询文档中的所有 sheet
    - sheetName → sheetId 映射
    """

    def __init__(self, base_id: str, operator_id: str, token_manager: DingTalkTokenManager = None):
        self.base_id = base_id
        self.operator_id = operator_id

        self.token_manager = token_manager or DingTalkTokenManager()
        self.access_token = self.token_manager.get_access_token()

        self.base_url = f"https://api.dingtalk.com/v1.0/notable/bases/{base_id}"
        self.headers = self._get_headers()
        self.params = {"operatorId": operator_id}

        self._sheet_cache: Optional[List[Dict[str, Any]]] = None

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "x-acs-dingtalk-access-token": self.access_token or ""
        }

    def _refresh_token(self):
        self.access_token = self.token_manager.get_access_token(force_refresh=True)
        self.headers = self._get_headers()

    def list_sheets(self, use_cache: bool = False) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/sheets"

        response = requests.get(
            url=url,
            headers=self.headers,
            params=self.params,
            timeout=30
        )

        print("HTTP status:", response.status_code)
        print("RAW response:", response.text)

        response.raise_for_status()
        result = response.json()

        print("JSON parsed:", result)

        sheets = result.get("sheets", [])
        return sheets

    def get_sheet_name_id_map(self) -> Dict[str, str]:
        """
        获取 sheetName -> sheetId 的映射
        """
        sheets = self.list_sheets()
        return {sheet["name"]: sheet["id"] for sheet in sheets}

    def get_sheet_id_by_name(self, sheet_name: str) -> Optional[str]:
        """
        根据 sheet 名称获取 sheetId
        """
        sheet_map = self.get_sheet_name_id_map()
        return sheet_map.get(sheet_name)

    def sheet_exists(self, sheet_name: str) -> bool:
        """
        判断 sheet 是否存在
        """
        return self.get_sheet_id_by_name(sheet_name) is not None

def query_sheet():
    sheet_manager = DingTalkSheetManager(
        base_id="XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL",
        operator_id="ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    )

    sheets = sheet_manager.list_sheets()

    for s in sheets:
        print(f"{s['name']} -> {s['id']}")


# 示例使用函数
def upload_multiple_records(config, records, logger):
    """
    批量上传多条记录的完整示例
    """
    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建上传器
    uploader = DingTalkSheetUploader(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    logger.info(f"准备上传 {len(records)} 条记录...")

    # 批量上传，使用增强版的上传方法
    results = uploader.upload_batch_records(
        records_data=records,
        batch_size=50,
        delay=0.2,
        max_retries=2,
        retry_failed=True  # 启用失败重试
    )

    # 分析结果 - 适配新的返回格式
    logger.info(f"\n上传统计:")
    logger.info(f"总记录数: {results.get('total_records', 0)}")
    logger.info(f"成功数量: {results.get('success_count', 0)}")
    logger.info(f"失败数量: {results.get('failed_count', 0)}")
    logger.info(f"成功批次: {len(results.get('batch_results', [])) - len(results.get('failed_batches', []))}")
    logger.info(f"失败批次: {len(results.get('failed_batches', []))}")

    # 如果有失败的记录，记录详细信息
    if results.get('failed_count', 0) > 0:
        logger.warning(f"发现 {results['failed_count']} 条失败记录")

        # 记录失败的具体原因
        for i, failed_batch in enumerate(results.get('failed_batches', [])):
            logger.warning(
                f"  失败批次 {failed_batch.get('batch_index', i + 1)}: {failed_batch.get('error', '未知错误')}")

        # 可以将失败的记录保存到文件，便于后续处理
        if results.get('failed_records'):
            failed_file = f"failed_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            try:
                with open(failed_file, 'w', encoding='utf-8') as f:
                    # 移除临时添加的_upload_id字段（如果有）
                    clean_records = []
                    for record in results['failed_records']:
                        if isinstance(record, dict) and '_upload_id' in record:
                            record_copy = record.copy()
                            del record_copy['_upload_id']
                            clean_records.append(record_copy)
                        else:
                            clean_records.append(record)
                    json.dump(clean_records, f, ensure_ascii=False, indent=2)
                logger.info(f"失败的记录已保存到: {failed_file}")
            except Exception as e:
                logger.error(f"保存失败记录时出错: {e}")

    # 检查是否有数据格式错误（如评论数字段包含非数字）
    if results.get('failed_count', 0) > 0:
        # 分析失败原因，找出可能的格式问题
        format_errors = []
        for failed_batch in results.get('failed_batches', []):
            error_msg = failed_batch.get('error', '')
            if 'invalid' in error_msg.lower() and 'field' in error_msg.lower():
                # 提取字段名
                import re
                field_match = re.search(r"field '([^']+)'", error_msg)
                if field_match:
                    field_name = field_match.group(1)
                    format_errors.append(field_name)

        if format_errors:
            logger.warning(f"可能的数据格式问题字段: {set(format_errors)}")
            logger.warning("提示：钉钉多维表的数字字段不能包含'+'等非数字字符")

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

def test_delete_records(config,logger):
    """测试删除功能"""

    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建删除器
    deleter = DingTalkSheetDeleter(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    logger.info("\n 删除所有记录")
    # 删除所有记录（谨慎使用！）
    # 注意：这里使用confirm=False，不会实际执行删除
    delete_all_result = deleter.delete_all_records(
        batch_size=50,
        delay=0.2,
        confirm=True  # 设置为True才会实际删除
    )
    logger.info(f"删除所有记录结果: {delete_all_result.get('message')}")

    return deleter


if __name__ == "__main__":
    # 测试Token管理器
    # test_token_manager()
    query_sheet()
    # 或者运行批量上传示例
    # upload_multiple_records()

    # 运行查询测试
    # query_instance = test_query_records()

    #运行删除测试
    # delete_instance = test_delete_records()
