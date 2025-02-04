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
