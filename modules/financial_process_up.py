import pandas as pd
from pathlib import Path
import openpyxl

import warnings
from openpyxl.styles.stylesheet import Stylesheet
from utils.logger import get_logger
from utils.dingding_doc import  DingTalkTokenManager,DingTalkSheetUploader
FINANCIAL_DIR = Path(__file__).resolve().parent.parent / "data" / "financial"

warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style*"
)


logger=get_logger('financial_process_up')

class TemuSettlementProcessor:
    # 你关心的 Sheet → handler
    SHEET_HANDLERS = {
        "交易结算": "process_trade",
        "结算-消费者退款金额": "process_refund",
        "消费者及履约保障-售后问题": "process_after_issue",
        "消费者及履约保障-售后补寄": "process_resend",
        "结算-非商责平台售后补贴金额": "process_subsidy",
        "非商责平台售后补贴调整": "process_adjust",
    }

    def __init__(self, data_dir: str, output_dir: str, month_str):
        self.month_str = month_str
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = get_logger('financial_process_up')

        # { shop: { sku: record } }
        self.shop_records_map = {}

    # ========= 通用工具 =========
    def to_number(self, value, default=0.0):
        try:
            if value in ("", None):
                return default
            return float(value)
        except Exception:
            return default

    def safe_int(self, value, default=0):
        try:
            return int(float(value))
        except Exception:
            return default

    # ========= 文件名解析 =========
    def parse_filename(self, filename: str):
        """
        101-Temu全托管_12_美国.xlsx
        """
        name = Path(filename).stem
        parts = name.split("_")
        shop = parts[0]
        self.logger.info(shop)
        region = parts[-1]
        return shop, region

    # ========= Sheet 扫描（性能关键） =========
    def scan_valid_sheets(self, file_path: Path):
        wb = openpyxl.load_workbook(file_path, read_only=True)
        aa = [name for name in wb.sheetnames
              if name in self.SHEET_HANDLERS]
        self.logger.info(aa)
        return aa

    # ========= SKU 初始化 =========
    def init_sku_record(self, sku, shop, region):
        return {
            "平台": "temu",
            "店铺": shop,
            # "区域": region,
            "sku货号": sku,

            "交易收入-销售数量": 0,
            "交易收入-收入金额": 0,

            "退款数量": 0,
            "退款金额-赔付金额": 0,

            "售后问题-数量": 0,
            "售后问题-赔付金额": 0,

            "售后补寄-数量": 0,
            "售后补寄-赔付金额": 0,

            "售后补贴数量": 0,
            "售后补贴-补贴金额": 0,
            "售后补贴-补贴金额调整": 0,
            "月份": self.month_str,

            "核价": []
        }

    # ========= 各 Sheet 处理器 =========
    def process_trade(self, df, records, shop, region):
        if "SKU货号" not in df.columns:
            return

        for _, row in df.iterrows():
            sku = row["SKU货号"]
            if not sku:
                continue

            qty = self.safe_int(row.get("数量"))
            amount = self.to_number(row.get("金额"))

            record = records.setdefault(
                sku, self.init_sku_record(sku, shop, region)
            )

            record["交易收入-销售数量"] += qty
            record["交易收入-收入金额"] += amount

            if qty > 0:
                record["核价"].append(amount / qty)

    def process_refund(self, df, records, shop, region):
        if "SKU货号" not in df.columns:
            return

        for _, row in df.iterrows():
            sku = row["SKU货号"]
            if not sku:
                continue

            refund = self.to_number(row.get("消费者退款金额"))

            record = records.setdefault(
                sku, self.init_sku_record(sku, shop, region)
            )

            record["退款数量"] += 1
            record["退款金额-赔付金额"] += refund

    def process_after_issue(self, df, records, shop, region):
        if "SKU货号" not in df.columns:
            return

        for _, row in df.iterrows():
            sku = row["SKU货号"]
            if not sku:
                continue

            amount = self.to_number(row.get("赔付金额"))

            record = records.setdefault(
                sku, self.init_sku_record(sku, shop, region)
            )

            record["售后问题-数量"] += 1
            record["售后问题-赔付金额"] += amount

    def process_resend(self, df, records, shop, region):
        if "SKU货号" not in df.columns:
            return

        for _, row in df.iterrows():
            sku = row["SKU货号"]
            if not sku:
                continue

            qty = self.safe_int(row.get("数量"))
            amount = self.to_number(row.get("赔付金额"))

            record = records.setdefault(
                sku, self.init_sku_record(sku, shop, region)
            )

            record["售后补寄-数量"] += qty
            record["售后补寄-赔付金额"] += amount

    def process_subsidy(self, df, records, shop, region):
        if "SKU货号" not in df.columns:
            return

        for _, row in df.iterrows():
            sku = row["SKU货号"]
            if not sku:
                continue

            amount = self.to_number(row.get("非商责平台售后补贴金额"))

            record = records.setdefault(
                sku, self.init_sku_record(sku, shop, region)
            )

            record["售后补贴数量"] += 1
            record["售后补贴-补贴金额"] += amount

    def process_adjust(self, df, records, shop, region):
        if "SKU货号" not in df.columns:
            return

        for _, row in df.iterrows():
            sku = row["SKU货号"]
            if not sku:
                continue

            amount = self.to_number(row.get("收支金额"))

            record = records.setdefault(
                sku, self.init_sku_record(sku, shop, region)
            )

            record["售后补贴-补贴金额调整"] += amount

    # ========= 单 Excel 入口 =========
    def process_excel(self, file_path: Path):
        shop, region = self.parse_filename(file_path.name)
        records = self.shop_records_map.setdefault(shop, {})

        valid_sheets = self.scan_valid_sheets(file_path)

        for sheet_name in valid_sheets:
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                dtype=str
            ).fillna("")

            handler = getattr(self, self.SHEET_HANDLERS[sheet_name])
            handler(df, records, shop, region)

    # ========= 批量执行 =========
    def run(self):
        for file in self.data_dir.glob("*.xlsx"):
            self.process_excel(file)

    # ========= 导出 =========
    def export(self):
        for shop, records in self.shop_records_map.items():
            df = pd.DataFrame(records.values())

            # ===== 保底列（非常关键）=====
            if "核价" not in df.columns:
                df["核价"] = [[] for _ in range(len(df))]

            # ===== 计算核价 =====
            df["平均核价"] = df["核价"].apply(
                lambda x: sum(x) / len(x) if isinstance(x, list) and x else 0
            )

            df["最低核价"] = df["核价"].apply(
                lambda x: min(x) if isinstance(x, list) and x else 0
            )

            # ===== 实际净收入 =====
            df["实际净收入"] = (
                    df.get("交易收入-收入金额", 0)
                    - df.get("退款金额-赔付金额", 0)
                    - df.get("售后问题-赔付金额", 0)
                    - df.get("售后补寄-赔付金额", 0)
                    + df.get("售后补贴-补贴金额", 0)
                    + df.get("售后补贴-补贴金额调整", 0)
            )

            df.to_excel(self.output_dir / f"{shop}_总表.xlsx", index=False)
            self.logger.info(f"{shop}_总表.xlsx--导出成功")


def financial_process_up(CONFIG_DIR):
    processor = TemuSettlementProcessor(
        data_dir=CONFIG_DIR,
        output_dir=CONFIG_DIR / "output",
        month_str='2025年11月'
    )
    # 处理 Excel 并导出总表
    # processor.run()
    # processor.export()

    # ===== 上传部分 =====
    # 配置参数（请替换为实际的 Base / Sheet / Operator）

    upload_config = {
        "base_id": "Gl6Pm2Db8Dn3ePLptjdrxGNYVxLq0Ee4",
        "sheet_id": "hERWDMS",# 利润表
        "operator_id": "ZiSpuzyA49UNQz7CvPBUvhwiEiE"
    }

    token_manager = DingTalkTokenManager()
    uploader = DingTalkSheetUploader(
        base_id=upload_config["base_id"],
        sheet_id=upload_config["sheet_id"],
        operator_id=upload_config["operator_id"],
        token_manager=token_manager
    )


    # 遍历每个店铺的总表上传
    for shop_file in (CONFIG_DIR / "output").glob("*_总表.xlsx"):
        df = pd.read_excel(shop_file, dtype=str).fillna("")
        # 转成字典列表
        # === 钉钉「利润表」真实存在的字段 ===
        DINGTALK_FIELDS = {
            "平台",
            "店铺",
            "sku货号",
            "交易收入-销售数量",
            "交易收入-收入金额",
            "退款数量",
            "退款金额-赔付金额",
            "售后问题-数量",
            "售后问题-赔付金额",
            "售后补寄-数量",
            "售后补寄-赔付金额",
            "售后补贴数量",
            "售后补贴-补贴金额",
            "售后补贴-补贴金额调整",
            "月份",
            # ⚠️ 注意：这里不要写「核价」「实际净收入」除非你真的在钉钉建了列
        }
        raw_records = df.to_dict(orient="records")

        records = []
        for row in raw_records:
            clean_row = {k: v for k, v in row.items() if k in DINGTALK_FIELDS}
            records.append(clean_row)

        # 可选：打印被丢弃字段（只打印一次）
        all_fields = set(raw_records[0].keys())
        dropped = all_fields - DINGTALK_FIELDS
        if dropped:
           logger.info(f"⚠️ 以下字段未上传（钉钉表不存在）: {dropped}")

        # print(records)
        logger.info(f"正在上传店铺: {shop_file.name}, 共 {len(records)} 条记录...")

        # 批量上传，每批 50 条，失败重试 2 次
        results = uploader.upload_batch_records(records, batch_size=50, delay=0.2, max_retries=2)

        # 打印上传结果
        success_count = sum(1 for r in results if r.get("success"))
        fail_count = len(results) - success_count
        logger.info(f"{shop_file.name} 上传完成: 成功 {success_count} 批, 失败 {fail_count} 批")

        if fail_count:
            for i, r in enumerate([r for r in results if not r.get("success")], 1):
                logger.info(f"  批次 {i} 失败原因: {r.get('message', '未知错误')}")


# if __name__ == '__main__':
#     filepath = FINANCIAL_DIR / '11月份'
#     financial_process_up(filepath)
