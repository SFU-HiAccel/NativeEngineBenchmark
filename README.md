# NativeEngineBenchmark

This repo includes the code and experiment in the paper Understanding the Performance of Spark Native Execution: The Good, the Bad, and How to Fix It

The experiment includes the performance evaluation of performance-critical operators microbenchmark and TPC-DS of Vanilla Spark, Spark+Velox backend, Spark+ClickHouse backend and Spark+DataFusion backend. 

# Installation of Spark & Accelerator

## Download Vanilla Spark

Please go to [SparkDownload](https://archive.apache.org/dist/spark/spark-3.3.1/)

## Download of Apache Gluten & Building of Velox and ClickHouse Backend

Please go to [Gluten](https://github.com/apache/incubator-gluten/) to download the version v1.1.1

For Velox build, please refer to [VeloxBuild](https://github.com/apache/incubator-gluten/blob/main/docs/get-started/Velox.md), For ClickHouse build, please refer to [ClickHouseBuild](https://github.com/apache/incubator-gluten/blob/main/docs/get-started/ClickHouse.md)


```
# Velox Build
./dev/builddeps-veloxbe.sh build_arrow
./dev/builddeps-veloxbe.sh build_velox
./dev/builddeps-veloxbe.sh build_gluten_cpp
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.3 -DskipTests


# ClickHouse Build
bash ./ep/build-clickhouse/src/build_clickhouse.sh
export MAVEN_OPTS="-Xmx8g -XX:ReservedCodeCacheSize=2g"
mvn clean install -Pbackends-clickhouse -Pspark-3.3 -DskipTests -Dcheckstyle.skip
ls -al backends-clickhouse/target/gluten-XXXXX-spark-3.3-jar-with-dependencies.jar
```


## Download and Build of Blaze Accelerator

Please refer to [Blaze](https://github.com/kwai/blaze), download the version of v4.0.0

```
git clone git@github.com:kwai/blaze.git
cd blaze
SHIM=spark-3.3 # or spark-3.0/spark-3.1/spark-3.2/spark-3.3/spark-3.4/spark-3.5
MODE=release # or pre
mvn clean package -P"${SHIM}" -P"${MODE}"
```

## Copy the jar to the Spark's jar directory

```
cp gluten-package-1.3.0-SNAPSHOT.jar  /spark-3.3.1/jars/
or
cp blaze-engine-spark-3.3-release-4.0.0-SNAPSHOT.jar /spark-3.3.1/jars/
```


# TPC-DS E2E Benchmark

## Generation of TPC-DS dataset
Please refer to: https://github.com/apache/incubator-gluten/blob/main/tools/workload/tpcds/README.md

or 

```
git clone https://github.com/satyakommula96/spark_benchmark.git
git submodule init
git submodule update
```
### spark-sql-perf

```bash
cd spark-sql-perf
cp /tmp/sbt/bin/sbt-launch.jar build/sbt-launch-0.13.18.jar
bin/run
sbt +package
```

### tpcds-kit

```bash
cd ../tpcds-kit/tools
make CC=gcc-9 OS=LINUX
```

#### Parquet

```bash
cd ../tpcds
#For generating ~100GB parquet data
./gendata_parquet.sh
# For runing all 99 TPC-DS Queries
./runtpch_parquet.sh
```

#### ORC

```bash
cd ../tpcds
#For generating ~100GB orc data
./gendata_orc.sh
# For runing all 99 TPC-DS Queries
./runtpch_orc.sh
```


## TPC-DS queries
The queries used in the experiment: please refer to: https://github.com/apache/incubator-gluten/tree/main/tools/gluten-it/common/src/main/resources/tpcds-queries

# Running Spark clusters

## Vanilla Spark

```
/spark-3.3.1-bin-hadoop2-ck/bin/spark-shell\
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.codegen.wholeStage=true \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --executor-cores 4 \
  --conf spark.local.dir=/localssd/hza214 \ 
  --conf spark.driver.memoryOverhead=4g\
  --conf spark.executor.memory=16g\
  --conf spark.executor.memoryOverhead=4g\
  --driver-memory 40g
```

## Spark + Velox

```
/spark-3.3.1-bin-hadoop3-velox/bin/spark-shell   --conf spark.gluten.enabled=true  
--conf spark.local.dir=/localssd/hza214
--conf spark.plugins=org.apache.gluten.GlutenPlugin
--conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.codegen.wholeStage=true \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=20g \
--executor-cores 4 \
--conf spark.local.dir=/localssd/hza214 \ 
--conf spark.driver.memoryOverhead=4g\
--conf spark.executor.memory=16g\
--conf spark.executor.memoryOverhead=4g\
--driver-memory 40g
```

## ClickHouse

```
/spark-3.3.1-bin-hadoop2-ck/bin/spark-shell\
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.codegen.wholeStage=true \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --conf spark.executorEnv.LD_PRELOAD=/localhdd/hza214/gluten/cpp-ch/build/utils/extern-local-engine/libch.so\
  --conf spark.gluten.sql.columnar.libpath=/localhdd/hza214/gluten/cpp-ch/build/utils/extern-local-engine/libch.so \
  --conf spark.gluten.sql.columnar.iterator=true \
  --conf spark.gluten.sql.columnar.loadarrow=false \
  --conf spark.gluten.sql.columnar.hashagg.enablefinal=true \
  --conf spark.gluten.sql.enable.native.validation=false \
  --conf spark.gluten.sql.columnar.forceShuffledHashJoin=true \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --executor-cores 4 \
  --conf spark.local.dir=/localssd/hza214 \ 
  --conf spark.driver.memoryOverhead=4g\
  --conf spark.executor.memory=16g\
  --conf spark.executor.memoryOverhead=4g\
  --driver-memory 40g
```


## Blaze

```
/spark-3.3.3-bin-hadoop3/bin/spark-shell \
  --conf spark.files.ignoreCorruptFiles=true\
  --conf spark.blaze.enable=true\
  --conf spark.sql.extensions=org.apache.spark.sql.blaze.BlazeSparkSessionExtension\
  --conf spark.shuffle.manager=org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleManager\
  --conf spark.driver.memory=20g\
  --conf spark.executor.cores=4 \
  --conf spark.driver.memoryOverhead=4g\
  --conf spark.executor.memory=16g\
  --conf spark.executor.memoryOverhead=4g\
  --conf spark.memory.offHeap.enabled=true\
  --conf spark.memory.offHeap.size=20g\
  --conf spark.local.dir=/localssd/hza214/sparktmp

```

## Run TPC-DS Queries
Execute run_tpcds_orc.scala or run_tpcds_parquet.scala, which will run all the TPC-DS queries and save the time to a txt file.
