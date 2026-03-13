from util.dingding_doc import upload_multiple_records
from util.logger import get_logger
from api.ymx_new_data_multithread import NewYmxNewData
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, current_thread
from auth.ymx_login import  MaiJiaLogin
import asyncio
from storage.ymx_data_process import DataProcessor

from pathlib import Path
async def main():
    client = MaiJiaLogin(headless=False)
    await client.login_and_save_cookie_dict()



def crawl_country(country_name, file_lock):
    """线程任务函数：爬取单个国家"""
    thread_name = current_thread().name
    print(f"[{thread_name}] 开始爬取{country_name}站点")

    try:
        ymx = NewYmxNewData(file_lock=file_lock)
        ymx.set_country(country_name)
        total_items = ymx.get_all_page(start_page=1, max_page=1000)

        print(f"[{thread_name}] √ {country_name}: 爬取成功，获取{total_items}条数据")
        return {
            "thread": thread_name,
            "country": country_name,
            "status": "success",
            "total_items": total_items
        }
    except Exception as e:
        print(f"[{thread_name}] × {country_name}: 爬取失败，错误: {e}")
        return {
            "thread": thread_name,
            "country": country_name,
            "status": "error",
            "error": str(e)
        }

def ymx_main_thread_pool(max_workers=3):
    """使用线程池的主程序入口"""
    print("=" * 60)
    print("亚马逊商品爬虫启动（线程池版）")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"线程池大小: {max_workers}")
    print(f"目标国家: 美国、英国、德国、法国、西班牙")
    print("=" * 60)
    print("分配任务中...")

    countries = ["美国", "英国", "德国", "法国", "西班牙"]

    # 创建文件锁，确保线程安全地写入文件
    file_lock = Lock()

    # 统计信息
    success_count = 0
    error_count = 0
    total_items_all = 0
    thread_results = {}

    print("\n启动线程池，开始并发爬取...")
    print("-" * 60)

    # 创建线程池
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="YmxThread") as executor:
        # 提交所有任务
        future_to_country = {
            executor.submit(crawl_country, country, file_lock): country
            for country in countries
        }

        # 显示线程分配信息
        print(f"任务分配完成:")
        for future, country in future_to_country.items():
            print(f"  - {country} -> 已提交到线程池")

        print("\n等待任务执行...")
        print("-" * 60)

        # 等待任务完成并处理结果
        completed_count = 0
        for future in as_completed(future_to_country):
            completed_count += 1
            country = future_to_country[future]

            try:
                result = future.result(timeout=300)  # 5分钟超时
                thread_name = result.get("thread", "未知线程")

                if result["status"] == "success":
                    success_count += 1
                    total_items = result.get("total_items", 0)
                    total_items_all += total_items
                    thread_results[thread_name] = {
                        "country": country,
                        "status": "成功",
                        "items": total_items
                    }
                    print(
                        f"[进度 {completed_count}/{len(countries)}] {thread_name}: √ {country} 完成，获取{total_items}条数据")
                else:
                    error_count += 1
                    thread_results[thread_name] = {
                        "country": country,
                        "status": "失败",
                        "error": result.get("error", "未知错误")
                    }
                    print(f"[进度 {completed_count}/{len(countries)}] {thread_name}: × {country} 失败")

            except Exception as e:
                error_count += 1
                print(f"[进度 {completed_count}/{len(countries)}] 处理{country}结果时出错: {e}")

    print("\n" + "=" * 60)
    print("所有国家爬取完成！")
    print("=" * 60)

    # 详细统计信息
    print("\n详细统计:")
    print("-" * 40)
    for thread_name, result in thread_results.items():
        if result["status"] == "成功":
            print(f"{thread_name}: {result['country']} - {result['status']} ({result['items']}条数据)")
        else:
            print(f"{thread_name}: {result['country']} - {result['status']} ({result.get('error', '未知错误')})")

    print("-" * 40)
    print(f"总结:")
    print(f"  成功: {success_count}个国家")
    print(f"  失败: {error_count}个国家")
    print(f"  总数据量: {total_items_all}条")
    print(f"  结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 保存统计结果到文件
    save_statistics(thread_results, total_items_all, success_count, error_count)

def save_statistics(thread_results, total_items, success_count, error_count):
    """保存爬取统计信息到文件"""
    current_date = datetime.now().strftime("%Y%m%d")
    stats_file = f"{Path(__file__).resolve().parent}/logs/ymx_stats_{current_date}.txt"

    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("亚马逊商品爬虫统计报告\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")

        f.write("线程执行详情:\n")
        f.write("-" * 40 + "\n")
        for thread_name, result in thread_results.items():
            if result["status"] == "成功":
                f.write(f"{thread_name}: {result['country']} - {result['status']} ({result['items']}条数据)\n")
            else:
                f.write(
                    f"{thread_name}: {result['country']} - {result['status']} ({result.get('error', '未知错误')})\n")

        f.write("\n" + "-" * 40 + "\n")
        f.write(f"总结统计:\n")
        f.write(f"  成功国家数: {success_count}\n")
        f.write(f"  失败国家数: {error_count}\n")
        f.write(f"  总数据量: {total_items}条\n")
        f.write("=" * 60 + "\n")

    print(f"统计信息已保存到: {stats_file}")



if __name__ == '__main__':

    logger = get_logger('YmxNews')
    logger.info('程序开始启动')

    config = {
        "base_id": "KGZLxjv9VG03dPLZt4B3yZgjJ6EDybno",
        "sheet_id": "电商平台选品1",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    # 记录总时间开始
    total_start_time = time.time()

    logger.info('---------------------------------开始登录获取cookies-----------------------------------')
    asyncio.run(main())


    logger.info('---------------------------------开始爬取YMX_new数据-----------------------------------')
    ymx_main_thread_pool(max_workers=5)  # 可以调整线程数

    logger.info('---------------------------------开始去重数据-----------------------------------')
    processor = DataProcessor()

    # 筛选新数据
    new_data = processor.filter_new_data()

    logger.info('---------------------------------开始构建上传的数据-----------------------------------')
    records = processor.build_records(new_data)

    processor.import_csv_to_product_monitor(new_data)

    logger.info('---------------------------------开始上传数据-----------------------------------')
    upload_multiple_records(config, records,logger)

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
