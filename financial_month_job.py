from modules.financial_data import TKLoginDownloadData
from utils.config_loader import get_shop_config
import asyncio

async def main():
    start_time = input('请输入你要查询的月份开始日期（如：2025-11-01）：')
    end_time = input('请输入你要查询的月份结束日期（如：2025-11-30）：')
    name_list = ["TK全托1401店", "TK全托408-LXZ", "TK全托407-huidan", "TK全托406-yuedongwan", "TK全托405-huanchuang",
                 "TK全托404-kedi", "TK全托403-juyule", "TK全托401-xiyue", "TK全托402-quzhi", "TK全托1402店"]

    name_list=["TK全托407-huidan"]
    for name in name_list:
        account = get_shop_config(name)

        t = TKLoginDownloadData(name, account,start_time,end_time)
        await t.run()


if __name__ == "__main__":
    asyncio.run(main())