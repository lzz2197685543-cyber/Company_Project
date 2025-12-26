from smt_socket import SMT_Stock
from smt_goods import SMT_Good
import time
import json
from logger_config import SimpleLogger
from dingding_doc import DingTalkSheetDeleter,DingTalkSheetUploader,DingTalkTokenManager
import pandas as pd
from datetime import datetime


def load_cookies(cookies_file):
    """加载cookies"""
    try:
        with open(cookies_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f'❌ 加载cookies失败: {e}')
        return {}

def run_stock_crawler(shop_name, cookies):
    """运行库存爬虫"""
    if shop_name not in cookies:
        print(f"❌ 店铺 {shop_name} 的cookie不存在，跳过库存爬取")
        return 0

    try:
        print(f"📊 开始爬取 {shop_name} 库存数据...")
        crawler = SMT_Stock(shop_name, cookies[shop_name])
        count = crawler.run()
        print(f"✅ {shop_name}: 成功爬取 {count} 条库存数据")
        return count
    except Exception as e:
        print(f"❌ {shop_name} 库存爬取失败: {e}")
        logger.error(f"❌ {shop_name} 库存爬取失败: {e}")
        return 0

def run_goods_crawler(shop_name):
    """运行商品爬虫"""
    try:
        print(f"🛍️ 开始爬取 {shop_name} 商品数据...")
        s = SMT_Good(shop_name)
        success = s.run()

        if success:
            print(f"✅ {shop_name}: 商品数据爬取成功")
            return 1
        else:
            print(f"❌ {shop_name}: 商品数据爬取失败")
            return 0
    except Exception as e:
        print(f"❌ {shop_name} 商品爬取失败: {e}")
        logger.error(f"❌ {shop_name} 商品爬取失败: {e}")
        return 0


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

    print(f"准备上传 {len(records)} 条记录...")

    # 批量上传，每批50条，批次间延迟0.2秒，失败时重试2次
    results = uploader.upload_batch_records(records, batch_size=50, delay=0.2, max_retries=2)

    # 分析结果
    successful_batches = [r for r in results if r.get("success")]
    failed_batches = [r for r in results if not r.get("success")]

    print(f"\n上传统计:")
    print(f"总批次: {len(results)}")
    print(f"成功批次: {len(successful_batches)}")
    print(f"失败批次: {len(failed_batches)}")

    if failed_batches:
        print(f"\n失败详情:")
        for i, failed in enumerate(failed_batches):
            print(f"  批次 {i + 1}: {failed.get('message', '未知错误')}")

    return results

def test_delete_records(config):

    # 创建Token管理器
    token_manager = DingTalkTokenManager()

    # 创建删除器
    deleter = DingTalkSheetDeleter(
        base_id=config["base_id"],
        sheet_id=config["sheet_id"],
        operator_id=config["operator_id"],
        token_manager=token_manager
    )

    print('开始删除数据')
    # 删除所有记录（谨慎使用！）
    # 注意：这里使用confirm=False，不会实际执行删除
    delete_all_result = deleter.delete_all_records(
        batch_size=50,
        delay=0.2,
        confirm=True  # 设置为True才会实际删除
    )
    print(f"删除所有记录结果: {delete_all_result.get('message')}")

    return deleter


def simple_match(shop_name):
    current_date = datetime.now().strftime("%Y%m%d")

    # 读取文件
    sku_df = pd.read_csv(f'./data/data/{shop_name}_goods_{current_date}.csv')  # 货号ID,sku
    main_df = pd.read_csv(f'./data/data/{shop_name}_stock_{current_date}.csv')  # 平台,店铺,货号ID,商品名称,...

    sku_df['货号ID'] = sku_df['货号ID'].astype(str)
    main_df['货号ID'] = main_df['货号ID'].astype(str)

    # 使用merge合并数据
    result_df = pd.merge(
        main_df,
        sku_df,
        on='货号ID',
        how='left'
    )

    records=[]
    # 修复：解包 iterrows() 返回的元组
    for index, row in result_df.iterrows():
        # 检查指定字段是否都为0
        if (row['今日销量'] == 0 and
                row['近7天销量'] == 0 and
                row['近30天销量'] == 0 and
                row['平台库存'] == 0 and
                row['在途库存'] == 0):
            continue  # 跳过这条记录

        record = {
            "商品名称": row['商品名称'],
            "抓取数据日期": row['抓取数据日期'],
            "今日销量": row['今日销量'],
            "近7天销量": row['近7天销量'],
            "近30天销量": row['近30天销量'],
            "平台库存": row['平台库存'],
            "平台": row['平台'],
            "在途库存": row['在途库存'],
            "sku": str(row['sku']) if not pd.isna(row['sku']) else "",
            "店铺": row['店铺'],
        }
        records.append(record)

    return records


if __name__ == '__main__':

    logger = SimpleLogger(name='run')
    logger.info('程序开始启动')

    config = {
        "base_id": "XPwkYGxZV3KRy1Gxfyb1E305VAgozOKL", # 文档ID
        "sheet_id": "销量与库存-日更",
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"    # 操作人ID
    }

    print('---------------------------------开始删除数据-----------------------------------')
    test_delete_records(config)

    # 店铺列表
    shop_name_list = ['SMT202', 'SMT214', 'SMT212', 'SMT204', 'SMT203', 'SMT201', 'SMT208']
    # shop_name_list=['SMT212']

    # 记录总时间开始
    total_start_time = time.time()
    total_stock_items = 0
    successful_goods_shops = 0
    successful_stock_shops = 0

    # 加载cookies
    cookies_file = './data/socket_cookies.json'
    all_cookies = load_cookies(cookies_file)

    if not all_cookies:
        print("❌ 无法加载cookies，程序退出")
        exit(1)

    # 处理每个店铺
    for index, shop_name in enumerate(shop_name_list):
        print(f"\n{'=' * 60}")
        print(f"🛍️ 开始处理店铺 {index + 1}/{len(shop_name_list)}: {shop_name}")
        print(f"{'=' * 60}")

        shop_start_time = time.time()

        try:
            # 先运行商品爬虫
            goods_result = run_goods_crawler(shop_name)
            if goods_result > 0:
                successful_goods_shops += 1

            # 添加延迟，避免频繁请求
            if index < len(shop_name_list) - 1:
                delay = 3
                print(f"⏳ 等待 {delay} 秒后处理库存数据...")
                time.sleep(delay)

            # 再运行库存爬虫
            stock_count = run_stock_crawler(shop_name, all_cookies)
            if stock_count > 0:
                successful_stock_shops += 1
                total_stock_items += stock_count

            time.sleep(5)

            print('---------------------------------开始匹配sku数据-----------------------------------')
            records=simple_match(shop_name)

            print('---------------------------------开始上传数据-----------------------------------')
            upload_multiple_records(config, records)

            logger.info(f'{shop_name}数据上传成功')
            print(f'{shop_name}数据上传成功')

            time.sleep(3)


        except Exception as e:
            print(f"❌ 处理店铺 {shop_name} 时发生异常: {e}")
            logger.error(f"❌ 处理店铺 {shop_name} 时发生异常: {e}")
            continue

        # 清理资源
        import gc

        gc.collect()

        # 计算店铺处理时间
        shop_time = time.time() - shop_start_time
        print(f"⏱️  店铺 {shop_name} 处理完成，耗时: {shop_time:.2f} 秒")

    # 计算总时间
    total_time = time.time() - total_start_time

    # 输出统计信息
    print(f"\n{'=' * 60}")
    print(f"🎉 所有店铺处理完成！")
    print(f"{'=' * 60}")
    print(f"📊 统计信息:")
    print(f"  总店铺数量: {len(shop_name_list)}")
    print(f"  成功处理商品数据的店铺: {successful_goods_shops}")
    logger.info(f"  成功处理商品数据的店铺: {successful_goods_shops}")
    print(f"  成功处理库存数据的店铺: {successful_stock_shops}")
    logger.info(f"  成功处理库存数据的店铺: {successful_stock_shops}")
    print(f"  总库存数据条目: {total_stock_items}")
    logger.info(f"  总库存数据条目: {total_stock_items}")
    print(f"⏱️  总耗时: {total_time:.2f} 秒")
    logger.info(f"⏱️  总耗时: {total_time:.2f} 秒")
    print(f"⏱️  平均每个店铺耗时: {total_time / len(shop_name_list):.2f} 秒")


    # 转换为时分秒格式
    hours, remainder = divmod(total_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        print(f"⏱️  总耗时: {int(hours)}小时 {int(minutes)}分 {int(seconds)}秒")
    elif minutes > 0:
        print(f"⏱️  总耗时: {int(minutes)}分 {int(seconds)}秒")
    else:
        print(f"⏱️  总耗时: {seconds:.2f}秒")

    print(f"⏱️  开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(total_start_time))}")
    logger.info(f"⏱️  开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(total_start_time))}")
    print(f"⏱️  结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
    logger.info(f"⏱️  结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")








