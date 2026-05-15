# Cloud Service Log Analytics

This project analyses a synthetic cloud service log dataset using cloud object storage, MapReduce-style baseline analytics, and a Ray-based parallel extension.

## Dataset

The dataset was uploaded to an Alibaba Cloud OSS bucket and then retrieved to the ECS processing environment as:

`data/cloud_service_logs.csv`

The local CSV file contains 50,000 log records excluding the header row.

## Project Structure, Outputs, Environment and Running

**Comp3041J_Group13_code**

  **data/**  
  &nbsp;&nbsp;cloud_service_logs.csv

  **mapreduce_baseline/**  
  &nbsp;&nbsp;mr_request_count.py  
  &nbsp;&nbsp;mr_server_errors.py  
  &nbsp;&nbsp;mr_slow_endpoints.py  
  &nbsp;&nbsp;outputs/  
  &nbsp;&nbsp;&nbsp;&nbsp;request_count_by_service.txt  
  &nbsp;&nbsp;&nbsp;&nbsp;server_error_count_by_service.txt  
  &nbsp;&nbsp;&nbsp;&nbsp;top10_slow_endpoints.txt

  **ray_extension/**  
  &nbsp;&nbsp;ray_degraded_services.py  
  &nbsp;&nbsp;outputs/  
  &nbsp;&nbsp;&nbsp;&nbsp;ray_service_stats.txt  
  &nbsp;&nbsp;&nbsp;&nbsp;ray_degraded_services.txt

  **validation_comparison/**  
  &nbsp;&nbsp;validate_results.py  
  &nbsp;&nbsp;outputs/  
  &nbsp;&nbsp;&nbsp;&nbsp;runtime_log.txt  
  &nbsp;&nbsp;&nbsp;&nbsp;validation_check.txt

  run_all.sh  
  README.md

**Environment**  
The implementation was run on Alibaba Cloud ECS using Ubuntu 22.04, Python 3.10, pandas, and Ray local mode.

**Running the Project**  

```bash
# Create and activate a Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install the required packages
pip install pandas ray

# Run all analytics scripts
bash run_all.sh
```

This script regenerates the MapReduce outputs, Ray outputs, validation result, and runtime log.
