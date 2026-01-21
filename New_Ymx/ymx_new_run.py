from dingding_doc import DingTalkSheetUploader,DingTalkTokenManager
from logger_config import SimpleLogger
from ymx_new_data_multithread import NewYmxNewData
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, current_thread
from login import MaiJiaLogin
import asyncio
from filter_new_data import DataProcessor


async def main():
    client = MaiJiaLogin(
        phone="BAK2023",
        password="lxz2026",
        headless=False
    )
    await client.login_and_save_cookie_dict("./data/sellersprite_cookie_dict.json")



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
    stats_file = f"./logs/ymx_stats_{current_date}.txt"

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


def upload_multiple_records(config, records):
    """
    批量上传多条记录 - 修复NaN问题版
    """
    token_manager = DingTalkTokenManager()
    uploader = DingTalkSheetUploader(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    logger.info(f"准备上传 {len(records)} 条记录...")

    # 关键修复：处理NaN值
    import math

    def fix_nan(obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            # 大时间戳转为字符串
            elif obj > 1e12:
                from datetime import datetime
                try:
                    return datetime.fromtimestamp(obj / 1000).strftime("%Y-%m-%d")
                except:
                    return str(obj)
        return obj

    # 预处理所有记录
    processed_records = []
    for record in records:
        new_record = {}
        for key, value in record.items():
            if isinstance(value, dict):
                new_record[key] = {k: fix_nan(v) for k, v in value.items()}
            else:
                new_record[key] = fix_nan(value)
        processed_records.append(new_record)

    logger.info(f"已完成数据预处理，修复了NaN和Infinity值")

    # 上传
    results = uploader.upload_batch_records(processed_records, batch_size=50, delay=0.2, max_retries=2)

    # 分析结果
    successful = [r for r in results if r.get("success")]

    logger.info(f"\n上传统计:")
    logger.info(f"总批次: {len(results)}")
    logger.info(f"成功批次: {len(successful)}")
    logger.info(f"失败批次: {len(results) - len(successful)}")

    # 如果还有失败，保存这些记录
    if len(successful) < len(results):
        failed_records = []
        for i, result in enumerate(results):
            if not result.get("success"):
                start_idx = i * 50
                end_idx = min(start_idx + 50, len(processed_records))
                failed_records.extend(processed_records[start_idx:end_idx])

        if failed_records:
            import json
            import os
            from datetime import datetime

            os.makedirs("failed_records", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = os.path.join("failed_records", f"final_failed_{timestamp}.json")

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(failed_records, f, ensure_ascii=False, indent=2)

            logger.info(f"仍有 {len(failed_records)} 条记录失败，已保存到: {filepath}")

    return results


def save_failed_records(failed_records):
    """
    简洁地保存失败记录到文件
    """
    import json
    import os
    from datetime import datetime

    # 创建目录
    os.makedirs("./data", exist_ok=True)

    # 生成文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"failed_records_{timestamp}.json"
    filepath = os.path.join("failed_records", filename)

    try:
        # 保存到JSON文件
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(failed_records, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"失败记录已保存到: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"保存失败记录失败: {e}")
        return None




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

    logger.info('---------------------------------开始登录获取cookies-----------------------------------')
    asyncio.run(main())


    logger.info('---------------------------------开始爬取temu_new数据-----------------------------------')
    ymx_main_thread_pool(max_workers=5)  # 可以调整线程数

    logger.info('---------------------------------开始去重数据-----------------------------------')
    processor = DataProcessor()

    # 筛选新数据
    new_data = processor.filter_new_data()

    logger.info('---------------------------------开始构建上传的数据-----------------------------------')
    records = processor.build_records(new_data)

    processor.import_csv_to_product_monitor(new_data)

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
