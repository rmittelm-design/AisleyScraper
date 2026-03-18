"""Mark known labels in classifier_validation_audit.csv by product_id."""

from __future__ import annotations

import csv
from pathlib import Path

CSV_PATH = Path("out/classifier_validation_audit.csv")

# product_id -> (actual_label, note)
LABELS = {
    "9142950265086": ("drop", "manual check: failed"),
    "9308351824118": ("drop", "manual check: failed"),
    "9308351234294": ("drop", "manual check: failed"),
    "7558942982278": ("drop", "manual check: failed"),
    "8075824234668": ("drop", "manual check: failed"),
    "8075825381548": ("drop", "manual check: failed"),
}


def main() -> int:
    rows = list(csv.DictReader(CSV_PATH.open("r", encoding="utf-8", newline="")))
    if not rows:
        print("No rows found.")
        return 1

    marked = 0
    for row in rows:
        pid = row.get("product_id", "")
        if pid not in LABELS:
            continue

        actual_label, note = LABELS[pid]
        predicted_keep = str(row.get("predicted_keep", "")).strip() == "1"
        is_correct = int((actual_label == "keep" and predicted_keep) or (actual_label == "drop" and not predicted_keep))

        row["actual_label"] = actual_label
        row["is_correct"] = str(is_correct)
        row["notes"] = note
        marked += 1

    with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Marked {marked} rows in {CSV_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
