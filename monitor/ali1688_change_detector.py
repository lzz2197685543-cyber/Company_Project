class Ali1688ChangeDetector:

    # -------------------------
    # SKU变化检测
    # -------------------------
    @staticmethod
    def detect(old, new):

        if not old:
            return {}

        result = {}


        # 价格变化
        result["price_changed"] = str(old["page_price"]) != str(new["page_price"])

        # SKU名称变化
        result["name_changed"] = old["sku_name"] != new["sku_name"]

        return result

    # -------------------------
    # 商品信息变化检测
    # -------------------------
    @staticmethod
    def detect_product(old, new):

        if not old:
            return {}

        result = {}

        # 商品类目变化
        result["category_changed"] = old["category"] != new["category"]

        # SKU数量变化
        result["sku_count_changed"] = old["sku_count"] != new["sku_count"]

        return result