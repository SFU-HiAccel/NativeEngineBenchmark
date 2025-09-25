# Case Study @ ORC TableScan Benchmark

## Data Generation

```
python3 generateOrc.py
```

## GPU RAPIDS Accelerator 

Please go to [RAPIDSDownload](https://nvidia.github.io/spark-rapids/docs/download.html) to download the RAPIDS Accelerator, then put the jar under the /jars directory of Spark.



```
wget https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.0/rapids-4-spark_2.12-24.08.1.jar


./spark-shell
--conf spark.executor.cores=4        
--conf spark.rapids.sql.concurrentGpuTasks=4        
--driver-memory 40g        
--conf spark.rapids.memory.pinnedPool.size=8G        
--conf spark.sql.files.maxPartitionBytes=512m        
--conf spark.plugins=com.nvidia.spark.SQLPlugin
```

After running the spark shell, run the runORC.scala code

## C++ Cudf benchmark

```
mkdir build && cd build
cmake ..
make
```


## Vanilla Spark/Spark+Velox/Spark+ClickHouse

The software and configs are the same as selectiveScanMicrobenchmark.
After running the spark shell, then run the runORC.scala code

## FORC Accelerator

1. Please go to [FORC](https://github.com/SFU-HiAccel/FORC) to build the FORC accelerator. And download and compile this version of Gluten [Gluten](https://github.com/DamonZhao-sfu/gluten/tree/velox-forc) with the velox-forc branch 


2. 
Please go to https://github.com/DamonZhao-sfu/gluten/tree/velox-forc.
```
git clone git@github.com:DamonZhao-sfu/gluten.git
git checkout -b velox-forc
```
Build the velox engine


2. Build the microbenchmark integrated with FORC accelerator. Please refer to [Microbenchmark](https://github.com/apache/incubator-gluten/blob/main/docs/developers/MicroBenchmarks.md) for further details.

```
./dev/buildbundle-veloxbe.sh --build_tests=ON --build_benchmarks=ON
```


The queries with ORC Scanning:
```
spark.read.format("orc").load(file).select("numbers").foreach(_ => ())
```

The queries with TPCDS: Please refer to [TPC-DS](https://github.com/apache/incubator-gluten/tree/main/tools/gluten-it/common/src/main/resources/tpcds-queries)

For details of generating substrait plan and intermedaite input conf/plan for the above query, please refer to the above linked documents. After getting simualted query's stageId and taskId, execute the following command:

```
./generic_benchmark \
--conf /absolute_path/to/conf_[stageId]_[partitionId].ini \
--plan /absolute_path/to/plan_[stageId]_[partitionId].json \
--data /absolut_path/to/xxx.orc
```

