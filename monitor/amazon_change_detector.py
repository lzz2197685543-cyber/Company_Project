class ChangeDetector:

    @staticmethod
    def detect(old_data, new_data):

        result = {
            "price_changed": False,
            "title_changed": False,
            "subcategorie_changed":False,
            "coupon_changed":False,
            "old_price": None,
            "old_title": None,
            "old_subcategory": None,
            "old_coupon": None,
        }

        if not old_data:
            return result

        result["old_price"] = old_data.get("price")
        result["old_title"] = old_data.get("title")
        result["old_subcategory"] = old_data.get("subcategorie_name")
        result["old_coupon"] = old_data.get("coupon")

        # 价格变化检测（处理 None 值）
        def to_float_or_none(value):
            """安全转换为浮点数，None或空字符串或无效字符串返回None"""
            if value is None or value == '':
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

        # 使用
        old_price = to_float_or_none(old_data.get("price"))
        new_price = to_float_or_none(new_data.get("price"))

        if old_price != new_price:
            result["price_changed"] = True

        # 标题变化
        if old_data["title"] != new_data["title"]:
            result["title_changed"] = True

        # 小类目变化
        if old_data["subcategorie_name"] != new_data["subcategorie_name"]:
            result["subcategorie_changed"] = True

        if old_data["coupon"] != new_data["coupon"]:
            result["coupon_changed"] = True



        return result