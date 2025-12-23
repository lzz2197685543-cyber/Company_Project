from modules.temu_violation_recored import Temu_ViolationRecored
from modules.funds_restriction import Temu_Funds_Restriction
import asyncio
from utils.logger import get_logger

logger = get_logger("Main")

async def main():
    shop_name_list = ["2106-Temu全托管","2105-Temu全托管","2108-Temu全托管","2107-Temu全托管","2102-Temu全托管",
                      "1108-Temu全托管","1107-Temu全托管","1106-Temu全托管","1105-Temu全托管","2103-Temu全托管",
                      "112-Temu全托管","151-Temu全托管家居","1104-Temu全托管","1102-Temu全托管","1103-Temu全托管",
                      "1101-Temu全托管","2101-Temu全托管KA","110-Temu全托管KA","109-Temu全托管KA","108-Temu全托管",
                      "107-Temu全托管","106-Temu全托管","105-Temu全托管","104-Temu全托管","103-Temu全托管","102-Temu全托管","101-Temu全托管"]

    for shop_name in shop_name_list:

        logger.info(f"开始爬取店铺------------------------------{shop_name}--------------------------违法资金的数据")

        t_vio = Temu_ViolationRecored(shop_name)
        await t_vio.run()

        logger.info(f"开始爬取店铺-------------------------------{shop_name}---------------------------资金限制的数据")
        t_fund = Temu_Funds_Restriction(shop_name)
        await t_fund.run()



if __name__ == "__main__":
    asyncio.run(main())
