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
/localhdd/hza214/spark-3.3.1-bin-hadoop2-ck/bin/spark-shell --conf spark.sql.adaptive.enabled=true --conf spark.sql.codegen.wholeStage=true --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=20g --executor-cores 4 --conf spark.local.dir=/localssd/hza214 --conf spark.default.parallelism=200 --conf spark.sql.shuffle.partitions=200 --conf spark.driver.memoryOverhead=4g --conf spark.executor.memory=16g --conf spark.executor.memoryOverhead=4g --driver-memory 40g
```

## Spark + Velox

```
./spark-shell --conf spark.gluten.enabled=true  --conf spark.plugins=org.apache.gluten.GlutenPlugin --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager --conf spark.sql.adaptive.enabled=true --conf spark.sql.codegen.wholeStage=true --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=20g --executor-cores 4 --conf spark.default.parallelism=200 --conf spark.sql.shuffle.partitions=200 --conf spark.driver.memoryOverhead=4g --conf spark.executor.memory=16g --conf spark.executor.memoryOverhead=4g --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" --driver-memory 40g 
```

## Velox dockerfile

```
FROM apache/gluten:vcpkg-centos-7

RUN source /opt/rh/devtoolset-11/enable && \
    git clone https://github.com/apache/incubator-gluten.git && \
    cd incubator-gluten && \
    ./dev/builddeps-veloxbe.sh --run_setup_script=OFF --enable_s3=ON --enable_gcs=ON --enable_abfs=ON --enable_vcpkg=ON --build_arrow=OFF && \
    mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests
```


sudo docker build --network=host -t glutenimage -f dockerfile .

sudo docker run -it  --network=host   -v /localhdd/hza214/spark-3.5.5-bin-hadoop3:/localhdd/hza214/spark-3.5.5-bin-hadoop3 -v /mnt/glusterfs/users/hza214/parquet_1T:/mnt/glusterfs/users/hza214/parquet_1T -v /localhdd/hza214/gluten:/localhdd/hza214/gluten -v /localssd/hza214:/localssd/hza214  glutenimage

## ClickHouse

ensc-rcl-3 as master, hacc-2, rcl2,rcl3,rcl4 as the worker

```
export LD_PRELOAD=/localhdd/hza214/incubator-gluten/cpp-ch/build/utils/extern-local-engine/libch.so
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

## Ablation Study

### Spark 4

```
/localhdd/hza214/spark-4.1.0-preview1-bin-hadoop3/bin/spark-shell \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.codegen.wholeStage=true \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --executor-cores 4 \
  --conf spark.local.dir=/localssd/hza214 \
  --conf spark.driver.memoryOverhead=4g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  --driver-memory 40g
```

### Spark 3.5



### Spark 3.4





## Run TPC-DS Queries
execute run_tpcds_orc.scala or run_tpcds_parquet.scala in the spark shell, which will run all the TPC-DS queries and save the time to a txt file.
