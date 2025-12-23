import asyncio
from modules.smt_goods import SMTGoodsSpider
from modules.smt_stock import SMTStockSpider
from modules.smt_goods_async import SMTGoodsSpiderAsync


async def main():
    spider_goods = SMTGoodsSpider("SMT202")
    await spider_goods.run()

    # spider_socket = SMTStockSpider("SMT202")
    # await spider_socket.run()


if __name__ == "__main__":
    asyncio.run(main())
