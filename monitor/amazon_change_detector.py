class ChangeDetector:

    @staticmethod
    def detect(old_data, new_data):

        result = {
            "price_changed": False,
            "title_changed": False,
            "old_price": None,
            "old_title": None
        }

        if not old_data:
            return result

        result["old_price"] = old_data.get("price")
        result["old_title"] = old_data.get("title")

        # 价格变化
        if float(old_data["price"]) != float(new_data["price"]):
            result["price_changed"] = True

        # 标题变化
        if old_data["title"] != new_data["title"]:
            result["title_changed"] = True

        return result