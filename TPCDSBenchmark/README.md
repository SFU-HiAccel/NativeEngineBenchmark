# Config

## ENV

```
export CXX=$(conda info --root)/envs/velox-build/bin/x86_64-conda-linux-gnu-g++
export CC=$(conda info --root)/envs/velox-build/bin/x86_64-conda-linux-gnu-gcc
export LD_LIBRARY_PATH=$(conda info --root)/envs/velox-build/lib:$LD_LIBRARY_PATH
export CPATH=$(conda info --root)/envs/velox-build/include
```



## Vanilla Spark

```
/spark-3.3.1-bin-hadoop2-ck/bin/spark-shell\
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.codegen.wholeStage=true \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --executor-cores 4 \
  --conf spark.local.dir=/localssd/hza214 \ 
  --conf spark.default.parallelism=200\
  --conf spark.sql.shuffle.partitions=200\
  --conf spark.driver.memoryOverhead=4g\
  --conf spark.executor.memory=16g\
  --conf spark.executor.memoryOverhead=4g\
  --driver-memory 40g
```

## Spark + Velox

```
./spark-shell --conf spark.gluten.enabled=true  --conf spark.plugins=org.apache.gluten.GlutenPlugin --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager --conf spark.sql.adaptive.enabled=true --conf spark.sql.codegen.wholeStage=true --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=20g --executor-cores 4 --conf spark.default.parallelism=200 --conf spark.sql.shuffle.partitions=200 --conf spark.driver.memoryOverhead=4g --conf spark.executor.memory=16g --conf spark.executor.memoryOverhead=4g --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" --driver-memory 40g 
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
  --conf spark.default.parallelism=200\
  --conf spark.sql.shuffle.partitions=200\
  --conf spark.driver.memoryOverhead=4g\
  --conf spark.executor.memory=16g\
  --conf spark.executor.memoryOverhead=4g\
  --driver-memory 40g
```

```
export LD_PRELOAD=/localhdd/hza214/incubator-gluten/cpp-ch/build/utils/extern-local-engine/libch.so
/localhdd/hza214/spark-3.5.5-bin-hadoop3/bin/spark-shell \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=10g \
```




## Blaze

```
/spark-3.3.3-bin-hadoop3/bin/spark-shell \
  --conf spark.files.ignoreCorruptFiles=true\
  --conf spark.blaze.enable=true\
  --conf spark.default.parallelism=200\
  --conf spark.sql.shuffle.partitions=200\
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
execute run_tpcds_orc.scala or run_tpcds_parquet.scala in the spark shell, which will run all the TPC-DS queries and save the time to a txt file.
