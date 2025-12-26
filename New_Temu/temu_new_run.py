from dingding_doc import DingTalkSheetUploader,DingTalkTokenManager
from logger_config import SimpleLogger
from temu_new_data_multithreading import TemuNews
from filter_new_data import TemuDataProcessor
from login import GeekBILogin
import traceback
from datetime import datetime
import time
import asyncio


async def main():
    client = GeekBILogin(
        phone="18929089237",
        password="lxz2580hh",
        headless=False
    )

    auth = await client.login()
    print("最终 Authorization：", auth)

def temu_new_run():
    """主程序入口"""
    try:
        logger.info("=" * 60)
        logger.info("Temu商品多线程爬虫启动")
        logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"线程数: 5")
        logger.info("=" * 60)

        # 创建爬虫实例，设置线程数
        temu_crawler = TemuNews(max_workers=5)

        # 执行多线程爬取
        temu_crawler.get_all_page_multithread()

        logger.info("=" * 60)
        logger.info("爬虫执行完成")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.error("\n用户中断程序")
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        traceback.print_exc()


def upload_multiple_records(config,records):
    """
    批量上传多条记录的完整示例
    """
    # 配置参数（请替换为实际值）


    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建上传器（不再需要手动传入access_token）
    uploader = DingTalkSheetUploader(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    logger.info(f"准备上传 {len(records)} 条记录...")

    # 批量上传，每批50条，批次间延迟0.2秒，失败时重试2次
    results = uploader.upload_batch_records(records, batch_size=50, delay=0.2, max_retries=2)

    # 分析结果
    successful_batches = [r for r in results if r.get("success")]
    failed_batches = [r for r in results if not r.get("success")]

    logger.info(f"\n上传统计:")
    logger.info(f"总批次: {len(results)}")
    logger.info(f"成功批次: {len(successful_batches)}")
    logger.info(f"失败批次: {len(failed_batches)}")

    if failed_batches:
        logger.info(f"\n失败详情:")
        for i, failed in enumerate(failed_batches):
            logger.info(f"  批次 {i + 1}: {failed.get('message', '未知错误')}")

    return results





if __name__ == '__main__':

    logger = SimpleLogger(name='run')
    logger.info('程序开始启动')

    config = {
        "base_id": "KGZLxjv9VG03dPLZt4B3yZgjJ6EDybno",
        "sheet_id": "电商平台选品1",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    # 记录总时间开始
    total_start_time = time.time()
    logger.info('---------------------------------先进行登录-----------------------------------')
    asyncio.run(main())


    logger.info('---------------------------------开始爬取temu_new数据-----------------------------------')
    temu_new_run()


    logger.info('---------------------------------开始去重数据-----------------------------------')
    # 创建处理器
    processor = TemuDataProcessor()

    # 筛选新数据
    new_data = processor.filter_new_data()

    logger.info('---------------------------------开始构建上传的数据-----------------------------------')
    records = processor.build_records()


    logger.info('---------------------------------开始上传数据-----------------------------------')
    upload_multiple_records(config, records)

    logger.info(f'数据上传成功')



    time.sleep(3)


    # 计算总时间
    total_time = time.time() - total_start_time

    # 输出统计信息
    logger.info(f"{'=' * 60}")
    logger.info(f"📊 统计信息:")
    logger.info(f"⏱️  总耗时: {total_time:.2f} 秒")
    logger.info(f"⏱️  开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(total_start_time))}")
    logger.info(f"⏱️  结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
