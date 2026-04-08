from dataclasses import dataclass

@dataclass
class Product:
    goods_id: str
    source: str = "temu"
    name: str = ""
    category: str = ""
    sub_category:str=""
    month_sale:str=""
    status: str = "pending"
    shop_id: str = ""
    retry_count: int = 0