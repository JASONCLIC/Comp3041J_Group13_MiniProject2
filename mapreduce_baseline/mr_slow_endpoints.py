import csv
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = BASE_DIR / "data" / "cloud_service_logs.csv"
OUTPUT_FILE = BASE_DIR / "mapreduce_baseline" / "outputs" / "top10_slow_endpoints.txt"

SLOW_THRESHOLD_MS = 800


def mapper(row):
    """
    Mapper logic:
    if response_time_ms > 800, emit ((service_name, endpoint), 1)
    otherwise emit nothing.
    """
    response_time = int(row["response_time_ms"])

    if response_time > SLOW_THRESHOLD_MS:
        key = f'{row["service_name"]},{row["endpoint"]}'
        return key, 1

    return None


def reducer(mapped_items):
    """
    Reducer logic:
    sum all slow-request values grouped by service_name and endpoint.
    """
    counts = defaultdict(int)

    for item in mapped_items:
        if item is None:
            continue

        endpoint_key, value = item
        counts[endpoint_key] += value

    return counts


def main():
    mapped_items = []

    with INPUT_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            mapped_items.append(mapper(row))

    reduced_counts = reducer(mapped_items)
    top10 = sorted(reduced_counts.items(), key=lambda x: (-x[1], x[0]))[:10]

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        for endpoint_key, count in top10:
            f.write(f"{endpoint_key}\t{count}\n")

    print(f"Top 10 slow endpoints output written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
