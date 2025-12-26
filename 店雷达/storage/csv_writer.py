# storage/csv_writer.py
import csv
import os


class CSVWriter:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._init_file()

    def _init_file(self):
        if not os.path.exists(self.file_path):
            with open(self.file_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "发现日期",
                    "来源",
                    "商品ID",
                    "产品名称",
                    "产品链接",
                    "类目",
                    "上架时间",
                    "总销量",
                    "站点"
                ])
                writer.writeheader()

    def write_rows(self, rows: list[dict]):
        if not rows:
            return

        with open(self.file_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writerows(rows)
