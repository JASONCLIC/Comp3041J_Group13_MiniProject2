# Cloud Service Log Analytics

This project analyses a synthetic cloud service log dataset using cloud object storage, MapReduce-style baseline analytics, and a Ray-based parallel extension.

## Dataset

The dataset was uploaded to an Alibaba Cloud OSS bucket and then retrieved to the ECS processing environment as:

data/cloud_service_logs.csv

The local CSV file contains 50,000 log records excluding the header row.

## Project Structure

Comp3041J_Group13_code/
├─ data/
│ └─ cloud_service_logs.csv
├─ mapreduce_baseline/
│ ├─ mr_request_count.py
│ ├─ mr_server_errors.py
│ ├─ mr_slow_endpoints.py
│ └─ outputs/
├─ ray_extension/
│ ├─ ray_degraded_services.py
│ └─ outputs/
├─ validation_comparison/
│ ├─ validate_results.py
│ └─ outputs/
│ ├─ runtime_log.txt
│ └─ validation_check.txt
├─ run_all.sh
└─ README.md

## MapReduce Baseline

mapreduce_baseline/outputs/
├─ request_count_by_service.txt
├─ server_error_count_by_service.txt
└─ top10_slow_endpoints.txt

## Ray Extension

The Ray script processes the dataset in chunks using Ray remote tasks. It combines partial service-level statistics and detects degraded services.

Ray outputs:

ray_extension/outputs/
├─ ray_service_stats.txt
└─ ray_degraded_services.txt
## Validation and Runtime

validation_comparison/outputs/
├─ runtime_log.txt
└─ validation_check.txt

## Environment

The implementation was run on Alibaba Cloud ECS using Ubuntu 22.04, Python 3.10, pandas, and Ray local mode.

## Running the Project

Create and activate a Python virtual environment:

python3 -m venv venv
source venv/bin/activate

Install the required packages:

pip install pandas ray

Run all analytics scripts:

bash run_all.sh

The script regenerates the MapReduce outputs, Ray outputs, validation result, and runtime log.
