from services.Chinese_Financial_Statements.base_financial_detail_service import BaseFinancialDetailService


class DouYin_FinancialDetailService(BaseFinancialDetailService):
    skiprows = 0

    COLUMN_SUM_MAP = {
        "平台服务费": "平台服务费",
        "佣金": "佣金",
        "站外推广费": "站外推广费",
    }

    SCENE_SUM_MAP = {
        "偏远地区物流服务": "偏远地区物流服务",
        "欠票扣款-商家开票": "欠票扣款-商家开票",
        "上门取件运费": "上门取件运费",
        "消费者赔付": "消费者赔付",
    }

    def process_file(self, file_name, period=None):
        file_path = self._resolve_file_path(file_name, period)
        df = self.load_file(file_path)

        summary = {}

        for report_key, col_name in self.COLUMN_SUM_MAP.items():
            if col_name not in df.columns:
                summary[report_key] = 0.0
                continue

            summary[report_key] = float(
                df[col_name].apply(self.clean_numeric).abs().sum()
            )

        scene_col = "动账场景"
        amount_col = "动账金额"

        if scene_col not in df.columns or amount_col not in df.columns:
            for report_key in self.SCENE_SUM_MAP.keys():
                summary[report_key] = 0.0
        else:
            df[scene_col] = df[scene_col].fillna("").astype(str).str.strip()
            df[amount_col] = df[amount_col].apply(self.clean_numeric)

            for report_key, scene_keyword in self.SCENE_SUM_MAP.items():
                filtered_df = df[
                    df[scene_col].str.contains(scene_keyword, na=False, regex=False)
                ]
                summary[report_key] = float(filtered_df[amount_col].abs().sum())

        return {
            "shop_name": file_name,
            "summary_dict": summary,
            "raw_df": df,
            "file_path": str(file_path),
        }