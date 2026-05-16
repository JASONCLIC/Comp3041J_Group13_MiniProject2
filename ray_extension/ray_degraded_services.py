import csv
from collections import defaultdict
from pathlib import Path
import ray

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = BASE_DIR / "data" / "cloud_service_logs.csv"
OUTPUT_FILE = BASE_DIR / "ray_extension" / "outputs" / "ray_degraded_services.txt"
STATS_FILE = BASE_DIR / "ray_extension" / "outputs" / "ray_service_stats.txt"

SLOW_THRESHOLD_MS = 800
SLOW_RATE_THRESHOLD = 0.20
SERVER_ERROR_RATE_THRESHOLD = 0.10
TIMEOUT_THRESHOLD = 5
CHUNK_SIZE = 5000


@ray.remote
def process_chunk(rows):
    partial = defaultdict(lambda: {
        "total": 0,
        "slow": 0,
        "server_error": 0,
        "timeout": 0
    })

    for row in rows:
        service = row["service_name"]
        status_code = int(row["status_code"])
        response_time = int(row["response_time_ms"])
        error_type = row["error_type"].strip()

        partial[service]["total"] += 1

        if response_time > SLOW_THRESHOLD_MS:
            partial[service]["slow"] += 1

        if status_code >= 500:
            partial[service]["server_error"] += 1

        if error_type == "Timeout":
            partial[service]["timeout"] += 1

    return dict(partial)


def read_chunks():
    chunks = []
    current_chunk = []

    with INPUT_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            current_chunk.append(row)

            if len(current_chunk) >= CHUNK_SIZE:
                chunks.append(current_chunk)
                current_chunk = []

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def merge_partials(partials):
    merged = defaultdict(lambda: {
        "total": 0,
        "slow": 0,
        "server_error": 0,
        "timeout": 0
    })

    for partial in partials:
        for service, stats in partial.items():
            merged[service]["total"] += stats["total"]
            merged[service]["slow"] += stats["slow"]
            merged[service]["server_error"] += stats["server_error"]
            merged[service]["timeout"] += stats["timeout"]

    return dict(merged)


def detect_degraded_services(merged):
    degraded = []

    for service, stats in sorted(merged.items()):
        total = stats["total"]
        slow_rate = stats["slow"] / total if total else 0
        server_error_rate = stats["server_error"] / total if total else 0
        timeout_count = stats["timeout"]

        reasons = []

        if slow_rate > SLOW_RATE_THRESHOLD:
            reasons.append("high slow request rate")

        if server_error_rate > SERVER_ERROR_RATE_THRESHOLD:
            reasons.append("high server error rate")

        if timeout_count >= TIMEOUT_THRESHOLD:
            reasons.append("repeated timeout errors")

        if reasons:
            degraded.append((service, "; ".join(reasons)))

    return degraded


def main():
    chunks = read_chunks()

    ray.init(
        num_cpus=2,
        include_dashboard=False,
        ignore_reinit_error=True,
        log_to_driver=False
    )

    futures = [process_chunk.remote(chunk) for chunk in chunks]
    partials = ray.get(futures)

    ray.shutdown()

    merged = merge_partials(partials)
    degraded = detect_degraded_services(merged)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with STATS_FILE.open("w", encoding="utf-8") as f:
        f.write("service_name,total,slow,server_error,timeout,slow_rate,server_error_rate\n")

        for service, stats in sorted(merged.items()):
            total = stats["total"]
            slow_rate = stats["slow"] / total if total else 0
            server_error_rate = stats["server_error"] / total if total else 0

            f.write(
                f"{service},{total},{stats['slow']},{stats['server_error']},"
                f"{stats['timeout']},{slow_rate:.4f},{server_error_rate:.4f}\n"
            )

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        for service, reason in degraded:
            f.write(f"{service},{reason}\n")

    print(f"Processed chunks: {len(chunks)}")
    print(f"Service statistics written to: {STATS_FILE}")
    print(f"Degraded services written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
