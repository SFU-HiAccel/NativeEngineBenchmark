import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.io.{File, FileWriter}

val numRows = 1024 * 1024 * 100
val width = 1
import scala.util.Random
import spark.implicits._

val filePath = "./velox_selectivity.txt"

val selectExpr = (1 to width).map(i => s"CAST(value AS STRING) c$i")
val valueCol = monotonically_increasing_id()
val df = spark.range(numRows).map(_ => Random.nextLong()).selectExpr(selectExpr: _*).withColumn("value", valueCol).sort("value")

// Helper method to save a DataFrame as ORC and Parquet tables
def saveAsTable(df: DataFrame): Unit = {
  df.write.mode("overwrite").orc("/localhdd/hza214/gluten/tools/workload/orcTable_1col")
  df.write.mode("overwrite").parquet("/localhdd/hza214/gluten/tools/workload/parquetTable_1col")
}

def time[R](block: => R): Double = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val elapsedTime = (t1 - t0) / 1e9
    elapsedTime
}

def clearAllCaches(dfs: DataFrame*): Unit = {
  dfs.foreach(_.unpersist())
  spark.catalog.clearCache()
}

def writeTimeToFile(filePath: String, content: String): Unit = {
    val file = new File(filePath)
    val fw = new FileWriter(file, true)
    try {
        fw.write(content)
    } finally {
        fw.close()
    }
}

def filterPushDownBenchmark(numRows: Int, description: String, whereExpr: String, selectExpr: String = "*", percent: Double): Unit = {
    val orcDF = spark.read.orc(s"/localhdd/hza214/gluten/tools/workload/orcTable_1col")
    val parquetDF = spark.read.parquet(s"/localhdd/hza214/gluten/tools/workload/parquetTable_1col")
    
    orcDF.createOrReplaceTempView("orcTable")
    parquetDF.createOrReplaceTempView("parquetTable")
    val orcElapsedTime = time {spark.sql(s"SELECT * FROM orcTable WHERE $whereExpr").foreach(_ => ())}
    writeTimeToFile(filePath, s"time of orc: $orcElapsedTime, percent $percent\n")

    val parquetTime = time {spark.sql(s"SELECT * FROM parquetTable WHERE $whereExpr").foreach(_ => ())}
    writeTimeToFile(filePath, s"time of parquet: $parquetTime, percent $percent\n")
    spark.catalog.dropTempView("orcTable")
    spark.catalog.dropTempView("parquetTable")
    xsclearAllCaches(orcDF, parquetDF)
}

val selectExpr = (1 to width).map(i => s"MAX(c$i)").mkString("", ",", ", MAX(value)")

saveAsTable(df)
Seq(0.001,0.01,0.1,1,10,100).foreach { percent =>
filterPushDownBenchmark(
    numRows,
    s"Select $percent% int rows (value < ${percent / 100.0 * numRows})",
    s"value < ${ percent / 100.0 * numRows }",
    selectExpr,
    percent
)
}


