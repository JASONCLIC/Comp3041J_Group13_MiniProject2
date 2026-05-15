#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"
source venv/bin/activate

mkdir -p mapreduce_baseline/outputs
mkdir -p ray_extension/outputs
mkdir -p validation_comparison/outputs

RUNTIME_LOG="validation_comparison/outputs/runtime_log.txt"

{
  echo "Runtime log"
  echo "==========="
  echo "Execution environment: Alibaba Cloud ECS, Ubuntu 22.04, 2 vCPU, 2 GiB RAM, Python 3.10, Ray local mode"
  echo "Dataset: data/cloud_service_logs.csv, 50000 records excluding header"
  echo ""
} > "$RUNTIME_LOG"

run_and_time() {
    local label="$1"
    local command="$2"

    echo "Running: $label"

    local start_ms
    local end_ms
    local duration_ms

    start_ms=$(date +%s%3N)
    eval "$command"
    end_ms=$(date +%s%3N)

    duration_ms=$((end_ms - start_ms))
    echo "$label: ${duration_ms} ms" >> "$RUNTIME_LOG"
}

run_and_time "MapReduce request count by service" "python mapreduce_baseline/mr_request_count.py"
run_and_time "MapReduce server error count by service" "python mapreduce_baseline/mr_server_errors.py"
run_and_time "MapReduce top 10 slow endpoints" "python mapreduce_baseline/mr_slow_endpoints.py"
run_and_time "Ray degraded service detection" "python ray_extension/ray_degraded_services.py"

python validation_comparison/validate_results.py

{
  echo ""
  echo "Python version: $(python --version)"
  echo "Pandas version: $(python -c 'import pandas; print(pandas.__version__)')"
  echo "Ray version: $(python -c 'import ray; print(ray.__version__)')"
} >> "$RUNTIME_LOG"

echo "All tasks completed."
echo "Runtime log written to $RUNTIME_LOG"
