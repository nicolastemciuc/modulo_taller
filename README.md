# Dataset Construction Pipeline

This repository contains the full pipeline for constructing datasets from Spark workload executions, network captures, and system traces.

## 1. Run the Workloads

Launch the Spark workloads:
```sh
workloads/spark/run_spark_workloads.sh
```

## 2. Collect Traces in Parallel

While workloads are running, start both collectors:
```sh
sudo python3 trace_collector/polling_traces.py
sudo python3 trace_collector/packet_traces.py
```

Stop them after the workloads finish.  
They will generate:
- polling_traces/ — multiple CSVs with polled system metrics.
- packet_traces.csv — packet-level capture.

## 3. Build the Dataset

Execute the following scripts in order:

1. Extract flows from packet traces
```sh
sudo python3 dataset_construction/flow_extraction.py
```

2. Merge with polled traces to add system features
```sh
sudo python3 dataset_construction/feature_extraction.py
```

3. Add historical features (previous K flows)
```sh
python3 dataset_construction/historical_information.py
```

4. Add regression and classification labels
```sh
python3 dataset_construction/labeling.py
```

Each step generates an intermediate CSV inside dataset_construction/ that feeds the next one, producing the final labeled dataset ready for model training.
