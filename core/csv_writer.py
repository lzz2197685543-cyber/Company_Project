import csv


def write_csv(filename,out_dir, rows):
    if not rows:
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    fname = out_dir / filename
    exists = fname.exists()

    with open(fname, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not exists:
            writer.writeheader()
        writer.writerows(rows)
