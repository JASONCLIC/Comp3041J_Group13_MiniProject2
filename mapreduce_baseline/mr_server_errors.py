import csv
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = BASE_DIR / "data" / "cloud_service_logs.csv"
OUTPUT_FILE = BASE_DIR / "mapreduce_baseline" / "outputs" / "server_error_count_by_service.txt"


def mapper(row):
    """
    Mapper logic:
    if status_code >= 500, emit (service_name, 1)
    otherwise emit nothing.
    """
    status_code = int(row["status_code"])

    if status_code >= 500:
        return row["service_name"], 1

    return None


def reducer(mapped_items):
    """
    Reducer logic:
    sum all server-error values grouped by service_name.
    """
    counts = defaultdict(int)

    for item in mapped_items:
        if item is None:
            continue

        service_name, value = item
        counts[service_name] += value

    return counts


def main():
    mapped_items = []

    with INPUT_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            mapped_items.append(mapper(row))

    reduced_counts = reducer(mapped_items)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        for service_name, count in sorted(reduced_counts.items(), key=lambda x: (-x[1], x[0])):
            f.write(f"{service_name}\t{count}\n")

    print(f"Server error count output written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
