from dotenv import load_dotenv
import os

# 店铺分组（后面自动分配用）
SHOP_GROUPS = [
    ["8249645", "8246209", "6665344"],
    ["4720369", "7822681", "6666485"],
    ["8368031", "8246006", "8625842"],
    ["8248727", "8369148", "7790171"],
    ["8371473", "9262177", "6726588"],
    ["8248902", "8248902", "8248832"],
]

# 店铺之间的映射
shop_id={
    "101":"8249645","2101":"8246209","1101":"6665344",
    "102":"4720369","2103":"7822681","1103":"6666485",
    "103":"8368031","110":"8246006","1107":"8625842",
    "104":"8248727","105":"8369148","1105":"7790171",
    "106":"8371473","2106":"9262177","1104":"6726588",
    "108":"8248902","1108":"8248902","109":"8248832",
}



# 每店每次采集数量
SHOP_DAILY_LIMIT =10

# 并发配置
MAX_WORKERS = 3


# 类目列表
# 用于品类选择页面（对应 get_today_categories）
CATEGORIES_FOR_CATEGORY_INPUT = [
    "新奇玩具",
    "艺术与工艺品",
    "拼插类玩具",
    "娃娃及配件",
    "遥控和应用程序控制的玩具汽车"
]

# 用于子类目选择页面（对应 get_today_sub_categories）
CATEGORIES_FOR_SUB_CATEGORY_INPUT = [
    "电子类玩具",
    "游戏及配件",
    "游戏配件",
    "卡牌游戏",
    "益智、科教玩具",
    "过家家",
    "拼图",
    "婴幼玩具",
    "运动户外用品",
    "玩具车",
    "收藏玩具",
    "节日聚会用品",
]



load_dotenv()

# DB_CONFIG = {
#     'host': os.getenv('DB_HOST', '127.0.0.1'),
#     'port': int(os.getenv('DB_PORT', 3306)),
#     'user': os.getenv('DB_USER', 'root'),
#     'password': os.getenv('DB_PASSWORD', '1234'),
#     'database': os.getenv('DB_NAME', 'py_spider'),
#     'charset': 'utf8mb4'
# }

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'rm-bp186omby3lautfn0no.mysql.rds.aliyuncs.com'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root_lxz'),
    'password': os.getenv('DB_PASSWORD', 'Lxz123456'),
    'database': os.getenv('DB_NAME', 'py_spider'),
    'charset': 'utf8mb4'
}