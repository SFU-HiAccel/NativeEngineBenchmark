import org.apache.spark.sql.SparkSession
import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

//val directory = new File("./")
val orcFiles = List(
    "/mnt/smartssd_0n/data_rows_20.orc",
    "/mnt/smartssd_0n/data_rows_20.orc",
    "/mnt/smartssd_0n/data_rows_21.orc",
    "/mnt/smartssd_0n/data_rows_22.orc",
    "/mnt/smartssd_0n/data_rows_23.orc",
    "/mnt/smartssd_0n/data_rows_24.orc",
    "/mnt/smartssd_0n/data_rows_25.orc",
    "/mnt/smartssd_0n/data_rows_26.orc",
    "/mnt/smartssd_0n/data_rows_27.orc",
    "/mnt/smartssd_0n/data_rows_28.orc",
    "/mnt/smartssd_0n/data_rows_29.orc"
)

orcFiles.foreach { file =>
    val startTime = System.nanoTime()
    spark.read.format("orc").load(file).select("numbers").foreach(_ => ())
    val endTime = System.nanoTime()
    val duration = endTime - startTime
    println(f"Run${duration/1e6}%.2f ms")
}


/localhdd/hza214/spark-3.3.1-bin-hadoop3-velox/bin/spark   --conf spark.gluten.enabled=true  --conf spark.gluten.sql.debug=false  --conf spark.default.parallelism=48  --conf spark.sql.shuffle.partitions=48  --conf spark.gluten.sql.injectNativePlanStringToExplain=false  --conf spark.local.dir=/localssd/hza214/sparktmp  --conf spark.plugins=org.apache.gluten.GlutenPlugin  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager  --conf spark.memory.offHeap.enabled=true   --conf spark.memory.offHeap.size=100g   --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"  --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"  --executor-cores 4   --driver-memory 20g   --executor-memory 16g   --conf spark.executor.memoryOverhead=4g   --conf spark.sql.optimizer.runtime.bloomFilter.enabled=true   --conf spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold=0