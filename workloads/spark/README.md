Spark Workload Runner

This script automates Spark workloads (KMeans, PageRank) and can optionally run helpers such as
packet capture or polling trace collectors. Each run is saved under:
/mnt/extradisk/spark_traces/<app>/<timestamp>

Usage
------
./run_spark_workloads.sh [--build] [--no-wait] [--slow] [--capture] [--poll]

Options
-------
--build       Build project JARs with sbt before running
--no-wait     Run detached (helpers keep running)
--slow        Use longer workloads
--capture     Start tcpdump_pfring capture
--poll        Start polling trace collector

Environment Variables
---------------------
SPARK_SUBMIT    (default /opt/spark/bin/spark-submit)
MASTER_URL      (default spark://192.168.60.81:7077)
TRACE_ROOT      (default /mnt/extradisk/spark_traces)
EXECUTOR_CORES, TOTAL_EXECUTOR_CORES, EXECUTOR_MEMORY

CAP_IFACE       (default ens3)
CAP_FILE_NAME   (default packets.log)
POLL_CMD        (default python3 polling_collect.py)
POLL_INTERVAL_MS (default 50)

Examples
--------
./run_spark_workloads.sh
./run_spark_workloads.sh --build --capture
./run_spark_workloads.sh --poll
./run_spark_workloads.sh --slow

Output
------
Each run directory under /mnt/extradisk/spark_traces/<app>/ includes:
  spark.out, spark.err, run.json, driver.pid, sysinfo.txt
  packets.log   (if capture enabled)
  polling/      (if polling enabled)

Cleanup
-------
If run with --no-wait:
  kill $(cat capture.pid)
  kill $(cat polling.pid)

Dependencies
------------
Spark, sbt, tcpdump_pfring, Python 3 + polling_traces, sudo privileges

License
-------
MIT – for benchmarking and research use.
