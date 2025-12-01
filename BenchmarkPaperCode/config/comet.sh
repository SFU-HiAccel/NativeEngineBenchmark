export COMET_JAR=/localhdd/hza214/datafusion-comet/spark/target/comet-spark-spark3.4_2.12-0.5.0-SNAPSHOT.jar
export SPARK_LOCAL_DIRS=/localssd/hza214/sparktmp
/localhdd/hza214/spark-3.4/spark-3.4.2-bin-hadoop3/bin/spark-shell \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.shuffle.mode=auto\
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.exec.all.enabled=true \
    --conf spark.comet.exec.shuffle.enabled=true \
    --conf spark.comet.shuffle.enforceMode.enabled=true \
    --conf spark.comet.cast.allowIncompatible=true \
    --conf spark.comet.explainFallback.enabled=true\
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager\
    --conf spark.comet.exec.shuffle.enabled=true\
    --conf spark.comet.exec.shuffle.mode=native\
    --conf spark.local.dir=/localssd/hza214/sparktmp \
    --conf spark.driver.memory=10G \
    --conf spark.executor.memoryOverhead=4g\
    --conf spark.executor.memory=16G \
    --executor-cores 4 \
    --conf spark.gluten.enabled=true \
    --conf spark.default.parallelism=48\
    --conf spark.sql.shuffle.partitions=48
