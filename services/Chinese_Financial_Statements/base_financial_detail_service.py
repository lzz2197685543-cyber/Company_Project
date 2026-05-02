from pathlib import Path
from datetime import datetime
import pandas as pd
import chardet


class BaseFinancialDetailService:
    KEYWORD_MAP = {}
    SPECIAL_KEYS = []

    income_col = "收入金额（+元）"
    expense_col = "支出金额（-元）"
    remark_col = "备注"

    skiprows = 4
    suffixes = [".csv", ".xlsx", ".xls"]

    def __init__(self, base_dir=None):
        if base_dir is None:
            self.data_dir = (
                Path(__file__).resolve().parent.parent.parent
                / "data"
                / "financial_details"
            )
        else:
            self.data_dir = Path(base_dir)

    @staticmethod
    def detect_encoding(file_path):
        with open(file_path, "rb") as f:
            result = chardet.detect(f.read())
        return result.get("encoding")

    @staticmethod
    def clean_numeric(val):
        if pd.isna(val) or str(val).strip() in ["", " "]:
            return 0.0

        cleaned = (
            str(val)
            .strip()
            .replace('"', "")
            .replace("\t", "")
            .replace(",", "")
            .replace("￥", "")
            .replace("¥", "")
        )

        if cleaned in ["", "-", "--"]:
            return 0.0

        try:
            return float(cleaned)
        except Exception:
            return 0.0

    def match_keyword(self, remark):
        if pd.isna(remark) or remark == "":
            return None

        remark = str(remark)

        for key, keywords in self.KEYWORD_MAP.items():
            for kw in keywords:
                if kw in remark:
                    return key

        return None

    def load_file(self, file_path):
        file_path = Path(file_path)
        suffix = file_path.suffix.lower()

        if suffix == ".csv":
            try:
                encoding = self.detect_encoding(file_path) or "gb18030"
                df = pd.read_csv(file_path, encoding=encoding, skiprows=self.skiprows, sep=",")
            except Exception:
                try:
                    df = pd.read_csv(file_path, encoding="utf-8-sig", skiprows=self.skiprows, sep=",")
                except Exception:
                    df = pd.read_csv(file_path, encoding="gb18030", skiprows=self.skiprows, sep=",")

        elif suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path, skiprows=self.skiprows)

        else:
            raise ValueError(f"不支持的文件格式: {file_path}")

        df.columns = (
            df.columns
            .astype(str)
            .str.strip()
            .str.replace('"', "", regex=False)
            .str.replace("\ufeff", "", regex=False)
        )

        return df

    def load_and_clean_file(self, file_path):
        df = self.load_file(file_path)

        for col in [self.income_col, self.expense_col, self.remark_col]:
            if col not in df.columns:
                raise KeyError(f"文件缺少必要列: {col}，当前列名: {list(df.columns)}")

        df[self.income_col] = df[self.income_col].apply(self.clean_numeric)
        df[self.expense_col] = df[self.expense_col].apply(self.clean_numeric)

        df["净金额"] = -df[self.income_col] + df[self.expense_col].abs()

        df[self.remark_col] = (
            df[self.remark_col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.replace('"', "", regex=False)
        )

        df["匹配键"] = df[self.remark_col].apply(self.match_keyword)

        return df

    def filter_special_keys(self, df):
        matched_df = df[df["匹配键"].notna()].copy()

        for special_key in self.SPECIAL_KEYS:
            if special_key not in matched_df["匹配键"].values:
                continue

            other_df = matched_df[matched_df["匹配键"] != special_key]
            special_df = matched_df[matched_df["匹配键"] == special_key]
            special_df = special_df[special_df[self.expense_col] != 0]

            matched_df = pd.concat([other_df, special_df], ignore_index=True)

        return matched_df

    def process_file(self, file_name, period=None):
        file_path = self._resolve_file_path(file_name, period)

        df = self.load_and_clean_file(file_path)
        matched_df = self.filter_special_keys(df)

        if matched_df.empty:
            summary = {}
        else:
            summary = matched_df.groupby("匹配键")["净金额"].sum().to_dict()

        return {
            "shop_name": file_name,
            "summary_dict": {k: float(v) for k, v in summary.items()},
            "matched_df": matched_df,
            "raw_df": df,
            "file_path": str(file_path),
        }

    def get_last_month_period(self):
        today = datetime.today()
        year = today.year
        month = today.month

        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1

        return f"{year}{month:02d}"

    def get_all_shop_summary(self, shop_names, period=None):
        if period is None:
            period = self.get_last_month_period()

        result = {}

        for shop_name in shop_names:
            processed = self.process_file(shop_name, period=period)
            result[shop_name] = processed["summary_dict"]

        return result

    def _resolve_file_path(self, file_name, period=None):
        if period:
            for suffix in self.suffixes:
                file_path = self.data_dir / f"{file_name}_{period}_账务明细{suffix}"
                if file_path.exists():
                    return file_path

            raise FileNotFoundError(f"文件不存在: {file_name}_{period}_账务明细.csv/xlsx/xls")

        files = []
        for suffix in self.suffixes:
            files.extend(self.data_dir.glob(f"{file_name}_*_账务明细{suffix}"))

        files = sorted(files, reverse=True)

        if not files:
            raise FileNotFoundError(f"未找到匹配文件: {file_name}_*_账务明细.csv/xlsx/xls")

        return files[0]