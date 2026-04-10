import json
from collections import Counter
from pathlib import Path

counter = Counter()
for line in Path("data/publish_fails.jsonl").open(encoding="utf-8"):
    record = json.loads(line)
    missing = record.get("missing_attr")
    if missing:
        counter[missing] += 1

print("缺失属性统计：")
for attr, cnt in counter.most_common():
    print(f"{attr}: {cnt}次")


