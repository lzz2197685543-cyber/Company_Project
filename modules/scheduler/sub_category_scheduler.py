# -*- coding: utf-8 -*-
"""
子类目调度模块（直接引入即可用）
用途：控制每天只跑部分二级类目，而不是全量跑

改进点：
1. 消除重复代码，统一内部调度逻辑
2. 使用基于绝对天数的调度算法，避免每月周期性不均匀覆盖
3. 支持依赖注入日期，便于测试
4. 命名更清晰，添加注释
5. 处理 size 超过类目池的情况
6. 增加日志输出
7. 示例函数中优化等待方式（建议替换固定等待）
"""

import datetime
import logging
from typing import List, Optional
from config.settings import CATEGORIES_FOR_SUB_CATEGORY_INPUT,CATEGORIES_FOR_CATEGORY_INPUT

# 配置简单日志（可替换为你的日志系统）
logger = logging.getLogger(__name__)


class SubCategoryScheduler:
    """二级类目调度器，按天均匀分配需要爬取的类目"""

    # ---------- 类目池配置 ----------
    # # 用于品类选择页面（对应 get_today_categories）
    # CATEGORIES_FOR_CATEGORY_INPUT = [
    #     "新奇玩具", "艺术与工艺品", "拼插类玩具", "娃娃及配件",
    #     "遥控和应用程序控制的玩具汽车"
    # ]
    #
    # # 用于子类目选择页面（对应 get_today_sub_categories）
    # CATEGORIES_FOR_SUB_CATEGORY_INPUT = [
    #     "电子类玩具", "游戏及配件", "游戏配件", "卡牌游戏",
    #     "益智、科教玩具", "过家家", "拼图", "婴幼玩具",
    #     "运动户外用品", "玩具车", "收藏玩具", "节日聚会用品",
    # ]

    # 调度参数：质数步长，保证均匀覆盖
    _STEP = 17

    @classmethod
    def _get_today_from_pool(
        cls,
        pool: List[str],
        size: int,
        reference_date: Optional[datetime.date] = None
    ) -> List[str]:
        """
        内部通用调度方法：从给定类目池中取出当天需要处理的类目

        :param pool: 类目列表
        :param size: 每天需要取出的类目数量
        :param reference_date: 参考日期，默认今天
        :return: 选中的类目列表
        """
        if reference_date is None:
            reference_date = datetime.date.today()

        total = len(pool)
        if size >= total:
            logger.warning(
                f"请求的 size={size} 超过类目池大小 {total}，返回全部类目"
            )
            return pool.copy()

        # 使用从固定起点开始的天数，避免每月重置导致的不均匀
        base_date = datetime.date(2020, 1, 1)
        days_since = (reference_date - base_date).days

        # 用质数步长打散，保证长期覆盖所有类目
        start = (days_since * cls._STEP) % total

        selected = []
        for i in range(size):
            idx = (start + i) % total
            selected.append(pool[idx])

        logger.info(
            f"[{reference_date}] 从 {len(pool)} 个类目中选中 {size} 个: {selected}"
        )
        return selected

    @classmethod
    def get_today_categories(cls, size: int = 4, reference_date: Optional[datetime.date] = None) -> List[str]:
        """获取今天需要爬取的一级类目"""
        # 直接使用模块级变量，不再通过 cls
        return cls._get_today_from_pool(CATEGORIES_FOR_CATEGORY_INPUT, size, reference_date)

    @classmethod
    def get_today_sub_categories(cls, size: int = 4, reference_date: Optional[datetime.date] = None) -> List[str]:
        """获取今天需要爬取的二级类目"""
        return cls._get_today_from_pool(CATEGORIES_FOR_SUB_CATEGORY_INPUT, size, reference_date)


# ================= 使用示例 =================

async def select_sub_categories(page):
    """
    在子类目选择页面中自动勾选今天需要处理的二级类目
    """
    sub_category_input = page.locator("#catIds").get_by_role("textbox")

    # 获取今天要跑的二级类目（每天1个）
    sub_categories = SubCategoryScheduler.get_today_sub_categories(size=1)

    # 某些类目在页面上有多个匹配项，需要指定索引
    nth_map = {
        "游戏及配件": 4,
        "游戏配件": 7,
        "拼图": 2,
        "玩具车": 2,
    }

    for name in sub_categories:
        nth_index = nth_map.get(name, 1)

        await sub_category_input.fill(name)
        await sub_category_input.press("Enter")

        # 点击匹配到的选项
        await page.get_by_text(name).nth(nth_index).click()

        # 优化等待：等待选项被选中（例如等待某个状态变化）
        # 如果页面有确认选中的元素，可替换为更可靠的等待
        # 例如：await page.wait_for_selector(f"text={name} >> nth={nth_index}", state="attached")
        await page.wait_for_timeout(300)  # 仍保留保底等待


async def select_categories(page):
    """
    在品类选择页面中自动勾选今天需要处理的一级类目
    """
    category_input = page.get_by_role("textbox", name="请选择品类")

    # 获取今天要跑的一级类目（每天1个）
    sub_categories = SubCategoryScheduler.get_today_categories(size=1)

    for name in sub_categories:
        await category_input.fill(name)
        await category_input.press("Enter")
        await page.get_by_text(name).nth(1).click()

        # 同样的优化建议
        await page.wait_for_timeout(300)

