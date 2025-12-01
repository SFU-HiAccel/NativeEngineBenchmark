export LD_PRELOAD=/localhdd/hza214/gluten/cpp-ch/build/utils/extern-local-engine/libch.so
/localhdd/hza214/spark-3.3.1-bin-hadoop2-ck/bin/spark-shell\
  --conf spark.serializer=org.apache.spark.serializer.JavaSerializer\
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.codegen.wholeStage=true \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=50g \
  --conf spark.shuffle.file.buffer=128k\
  --conf spark.gluten.sql.columnar.columnarToRow=true \
  --conf spark.executorEnv.LD_PRELOAD=/localhdd/hza214/gluten/cpp-ch/build/utils/extern-local-engine/libch.so\
  --conf spark.gluten.sql.columnar.libpath=/localhdd/hza214/gluten/cpp-ch/build/utils/extern-local-engine/libch.so \
  --conf spark.gluten.sql.columnar.iterator=true \
  --conf spark.gluten.sql.columnar.loadarrow=false \
  --conf spark.gluten.sql.columnar.hashagg.enablefinal=true \
  --conf spark.gluten.sql.enable.native.validation=false \
  --conf spark.gluten.sql.columnar.forceShuffledHashJoin=true \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog \
  --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"\
  --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"\
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --executor-cores 4 \
  --driver-memory 10g \
  --executor-memory 16g





