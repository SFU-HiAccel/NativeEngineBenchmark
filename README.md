# NativeEngineBenchmark



# Installation of Spark & Accelerator

## Download Vanilla Spark

Please go to [SparkDownload](https://archive.apache.org/dist/spark/spark-3.3.1/)

## Download of Apache Gluten & Building of Velox and ClickHouse Backend

For Velox build, please refer to [VeloxBuild](https://github.com/apache/incubator-gluten/blob/main/docs/get-started/Velox.md), For ClickHouse build, please refer to [ClickHouseBuild](https://github.com/apache/incubator-gluten/blob/main/docs/get-started/ClickHouse.md)


```
# Velox Build
./dev/builddeps-veloxbe.sh build_arrow
./dev/builddeps-veloxbe.sh build_velox
./dev/builddeps-veloxbe.sh build_gluten_cpp
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.3 -DskipTests


# ClickHouse Build
bash ./ep/build-clickhouse/src/build_clickhouse.sh
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