from datetime import datetime
from typing import List, Dict
from utils.dingtalk_bot import ding_bot_send
import re


class NotificationManager:
    """消息通知管理器"""

    def __init__(self, logger):
        self.logger = logger
        self.today = datetime.now().strftime("%m月%d号")

        # 需要关注的账号列表
        self.target_accounts = {
            "BAKAM账号-UK", "BAKAM账号-US", "Kidzbuddy账号-UK",
            "Kidzbuddy账号-US", "Meemazi-UK", "Meemazi-US",
            "Ninigai-CA", "Ninigai-UK", "Ninigai-US",
            "YYDeek账号-UK", "YYDeek账号-US"
        }

    def _get_account_base_name(self, account_name: str) -> str:
        """从完整账号名中提取基础账号名（去掉站点后缀）"""
        # 使用正则表达式匹配账号基础名
        pattern = r'^(.*?)(?:账号)?-(?:UK|US|CA|UK账号|US账号|CA账号)?$'
        match = re.match(pattern, account_name)
        if match:
            base_name = match.group(1)
            # 如果基础名不包含"账号"，加上"账号"
            if "账号" not in base_name:
                return f"{base_name}账号"
            return base_name
        return account_name

    def _group_by_base_account(self, messages_dict: Dict) -> Dict:
        """按基础账号名分组，合并不同站点的数据"""
        grouped_dict = {}

        for full_account, items in messages_dict.items():
            base_account = self._get_account_base_name(full_account)

            if base_account not in grouped_dict:
                grouped_dict[base_account] = {}

            # 提取站点后缀
            if "-UK" in full_account:
                site = "UK"
            elif "-US" in full_account:
                site = "US"
            elif "-CA" in full_account:
                site = "CA"
            else:
                site = "其他"

            # 存储站点数据
            grouped_dict[base_account][site] = {
                'full_account': full_account,
                'items': items
            }

        return grouped_dict

    def _get_country_threshold(self, account_name: str) -> int:
        """根据账号名称获取断货阈值"""
        if "-UK" in account_name:
            return 90  # 英国站点90天
        elif "-US" in account_name or "-CA" in account_name:
            return 60  # 美国和加拿大站点60天
        else:
            # 默认值，如果账号格式不符合预期
            return 60

    def _parse_number(self, value) -> int:
        """
        解析数字值，处理字符串和整数类型
        如果值为0或无法解析，返回None
        """
        if value is None:
            return None

        # 如果是整数类型
        if isinstance(value, int):
            return value if value > 0 else None

        # 如果是字符串类型
        if isinstance(value, str):
            # 移除所有非数字字符（除了负号）
            digits = ''.join(c for c in value if c.isdigit() or c == '-')
            if digits and digits != '-':  # 确保不是空字符串或只有负号
                num = int(digits)
                return num if num > 0 else None

        # 如果是浮点数类型
        if isinstance(value, float):
            num = int(value)
            return num if num > 0 else None

        return None

    def process_items_for_notifications(self, items: List[Dict]) -> tuple:
        """
        处理数据生成两种通知
        返回: (断货通知消息, 可售天数预警消息)
        """
        # 初始化消息字典
        stockout_messages = {}
        fba_warning_messages = {}

        for item in items:
            account = item.get("账号", "")

            # 只处理目标账号
            if account not in self.target_accounts:
                continue

            try:
                # 解析数据
                sku = item.get("sku", "")
                product_name = item.get("品名", "")
                # 检查品名是否为空
                if not product_name or str(product_name).strip() == "":
                    self.logger.debug(f"跳过品名为空的SKU: {sku}, 账号: {account}")
                    continue

                # 处理可售天数
                fba_days_raw = item.get("可售天数", "0")
                fba_days = self._parse_number(fba_days_raw)

                # 处理断货时间
                stockout_days_raw = item.get("断货时间", "0")
                stockout_days = self._parse_number(stockout_days_raw)

                # 如果值为0或None，跳过不处理
                if fba_days is None and stockout_days is None:
                    continue

                # 1. 可售天数(FBA)预警通知 (所有站点 <= 30天)
                if fba_days is not None and fba_days <= 30:
                    if account not in fba_warning_messages:
                        fba_warning_messages[account] = []
                    fba_warning_messages[account].append(
                        f"{product_name}/{fba_days}天"
                    )

                # 2. 断货通知 (根据站点不同阈值)
                if stockout_days is not None:
                    threshold = self._get_country_threshold(account)
                    if stockout_days <= threshold:
                        if account not in stockout_messages:
                            stockout_messages[account] = []
                        stockout_messages[account].append(
                            f"{product_name}/{stockout_days}天"
                        )

            except (ValueError, TypeError, Exception) as e:
                self.logger.warning(f"处理数据时出错: {item}, 错误: {str(e)}")
                continue

        # 格式化消息
        formatted_stockout_msgs = self._format_stockout_messages(stockout_messages)
        formatted_fba_msgs = self._format_fba_warning_messages(fba_warning_messages)

        return formatted_stockout_msgs, formatted_fba_msgs

    def _format_stockout_messages(self, messages_dict: Dict) -> List[str]:
        """格式化断货通知消息 - 按基础账号分组"""
        if not messages_dict:
            return ["无断货预警SKU"]

        # 按基础账号分组
        grouped_accounts = self._group_by_base_account(messages_dict)
        formatted_messages = []

        for base_account, sites_data in grouped_accounts.items():
            # 检查是否有任何站点的数据
            has_data = any(sites_data[site]['items'] for site in sites_data if sites_data[site]['items'])
            if not has_data:
                continue

            message_lines = [f"{base_account}（{self.today}）即将断货sku："]

            # 遍历所有站点（按UK, US, CA顺序）
            for site in ['UK', 'US', 'CA', '其他']:
                if site in sites_data and sites_data[site]['items']:
                    full_account = sites_data[site]['full_account']
                    message_lines.append(f"\n【{site}站点】")
                    message_lines.append("品名/断货时间")
                    message_lines.extend(sites_data[site]['items'])

            formatted_messages.append("\n".join(message_lines))

        return formatted_messages if formatted_messages else ["无断货预警SKU"]

    def _format_fba_warning_messages(self, messages_dict: Dict) -> List[str]:
        """格式化可售天数预警消息 - 按基础账号分组"""
        if not messages_dict:
            return ["无可售天数预警SKU"]

        # 按基础账号分组
        grouped_accounts = self._group_by_base_account(messages_dict)
        formatted_messages = []

        for base_account, sites_data in grouped_accounts.items():
            # 检查是否有任何站点的数据
            has_data = any(sites_data[site]['items'] for site in sites_data if sites_data[site]['items'])
            if not has_data:
                continue

            message_lines = [f"{base_account}（{self.today}）fba即将不可售sku："]

            # 遍历所有站点（按UK, US, CA顺序）
            for site in ['UK', 'US', 'CA', '其他']:
                if site in sites_data and sites_data[site]['items']:
                    full_account = sites_data[site]['full_account']
                    message_lines.append(f"\n【{site}站点】")
                    message_lines.append("品名/可售天数(FBA)天数")
                    message_lines.extend(sites_data[site]['items'])

            formatted_messages.append("\n".join(message_lines))

        return formatted_messages if formatted_messages else ["无可售天数预警SKU"]

    def send_to_wechat_group(self, messages: List[str], notification_type: str):
        """发送到钉钉群"""
        if not messages:
            if notification_type == "断货":
                self.logger.info("断货通知：无需发送通知")
            else:
                self.logger.info("FBA预警通知：无需发送通知")
            return

        # 检查是否是无数据的消息
        if len(messages) == 1 and ("无断货预警SKU" in messages[0] or "无可售天数预警SKU" in messages[0]):
            if notification_type == "断货":
                self.logger.info("断货通知：无需要预警的SKU")
            else:
                self.logger.info("FBA预警通知：无需要预警的SKU")
            return

        # 日志输出
        self.logger.info(f"\n{'=' * 50}")
        self.logger.info(f"{notification_type}通知内容:")
        self.logger.info(f"{'=' * 50}")

        # 构建钉钉消息
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 钉钉消息格式
        ding_msg = f"{'=' * 30}\n"
        ding_msg += f"📢 {notification_type}通知\n"
        ding_msg += f"⏰ 时间：{current_time}\n"
        ding_msg += f"{'=' * 30}\n\n"

        for i, message in enumerate(messages):
            ding_msg += f"{message}\n"
            # 如果不是最后一条，添加分隔线
            if i < len(messages) - 1:
                ding_msg += f"{'-' * 20}\n\n"

            self.logger.info(f"\n{message}\n")

        ding_msg += f"\n{'=' * 30}\n"
        ding_msg += "✅ 通知发送完成"

        # 发送到钉钉
        try:
            ding_bot_send('Amazon_StockBo', ding_msg)
            self.logger.info("✓ 钉钉消息发送成功")
        except Exception as e:
            self.logger.error(f"✗ 发送钉钉消息失败: {e}")

        self.logger.info(f"{'=' * 50}")