import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = BASE_DIR / "data" / "cloud_service_logs.csv"

REQUEST_COUNT_FILE = BASE_DIR / "mapreduce_baseline" / "outputs" / "request_count_by_service.txt"
SERVER_ERROR_FILE = BASE_DIR / "mapreduce_baseline" / "outputs" / "server_error_count_by_service.txt"
RAY_STATS_FILE = BASE_DIR / "ray_extension" / "outputs" / "ray_service_stats.txt"

OUTPUT_FILE = BASE_DIR / "validation_comparison" / "outputs" / "validation_check.txt"

TARGET_SERVICE = "payment-service"
SLOW_THRESHOLD_MS = 800


def read_key_value_file(path):
    values = {}

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:
                values[parts[0]] = int(parts[1])

    return values


def read_ray_stats(path):
    stats = {}

    with path.open("r", encoding="utf-8") as f:
        next(f)

        for line in f:
            service_name, total, slow, server_error, timeout, slow_rate, server_error_rate = line.strip().split(",")

            stats[service_name] = {
                "total": int(total),
                "slow": int(slow),
                "server_error": int(server_error),
                "timeout": int(timeout),
                "slow_rate": float(slow_rate),
                "server_error_rate": float(server_error_rate),
            }

    return stats


def scan_service_from_csv():
    stats = {
        "total": 0,
        "slow": 0,
        "server_error": 0,
        "timeout": 0,
    }

    with INPUT_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if row["service_name"] != TARGET_SERVICE:
                continue

            status_code = int(row["status_code"])
            response_time = int(row["response_time_ms"])
            error_type = row["error_type"].strip()

            stats["total"] += 1

            if response_time > SLOW_THRESHOLD_MS:
                stats["slow"] += 1

            if status_code >= 500:
                stats["server_error"] += 1

            if error_type == "Timeout":
                stats["timeout"] += 1

    stats["slow_rate"] = stats["slow"] / stats["total"]
    stats["server_error_rate"] = stats["server_error"] / stats["total"]

    return stats


def main():
    request_counts = read_key_value_file(REQUEST_COUNT_FILE)
    server_error_counts = read_key_value_file(SERVER_ERROR_FILE)
    ray_stats = read_ray_stats(RAY_STATS_FILE)
    csv_stats = scan_service_from_csv()

    reasons = []

    if csv_stats["slow_rate"] > 0.20:
        reasons.append("high slow request rate")

    if csv_stats["server_error_rate"] > 0.10:
        reasons.append("high server error rate")

    if csv_stats["timeout"] >= 5:
        reasons.append("repeated timeout errors")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        f.write(f"Validation service: {TARGET_SERVICE}\n\n")

        f.write("Independent CSV check\n")
        f.write(f"total_requests: {csv_stats['total']}\n")
        f.write(f"slow_requests: {csv_stats['slow']}\n")
        f.write(f"server_errors: {csv_stats['server_error']}\n")
        f.write(f"timeout_errors: {csv_stats['timeout']}\n")
        f.write(f"slow_request_rate: {csv_stats['slow_rate']:.4f}\n")
        f.write(f"server_error_rate: {csv_stats['server_error_rate']:.4f}\n\n")

        f.write("Output comparison\n")
        f.write(f"mapreduce_request_count: {request_counts[TARGET_SERVICE]}\n")
        f.write(f"mapreduce_server_errors: {server_error_counts[TARGET_SERVICE]}\n")
        f.write(f"ray_total_requests: {ray_stats[TARGET_SERVICE]['total']}\n")
        f.write(f"ray_slow_requests: {ray_stats[TARGET_SERVICE]['slow']}\n")
        f.write(f"ray_server_errors: {ray_stats[TARGET_SERVICE]['server_error']}\n")
        f.write(f"ray_timeout_errors: {ray_stats[TARGET_SERVICE]['timeout']}\n\n")

        f.write("Consistency check\n")
        f.write(f"total_requests_match: {csv_stats['total'] == request_counts[TARGET_SERVICE] == ray_stats[TARGET_SERVICE]['total']}\n")
        f.write(f"server_errors_match: {csv_stats['server_error'] == server_error_counts[TARGET_SERVICE] == ray_stats[TARGET_SERVICE]['server_error']}\n")
        f.write(f"slow_requests_match: {csv_stats['slow'] == ray_stats[TARGET_SERVICE]['slow']}\n")
        f.write(f"timeout_errors_match: {csv_stats['timeout'] == ray_stats[TARGET_SERVICE]['timeout']}\n\n")

        f.write("Degraded-service check\n")
        f.write(f"degraded: {bool(reasons)}\n")
        f.write(f"reason: {'; '.join(reasons)}\n")

    print(f"Validation output written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
