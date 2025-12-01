import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.Column  // Add this import
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

def generateAndLoadParquetTable(numRows: Int, outputPath: String = "/tmp/parquet_example"): Unit = {
  // Create Spark session if it doesn't exist
  import spark.implicits._

  // Create directory if it doesn't exist
  val dir = new File(outputPath)
  if (!dir.exists()) dir.mkdirs()
  
  // Generate DataFrame with single integer column
  val df = spark.range(numRows)
    .map(_ => scala.util.Random.nextInt()) // Each value will be random
    .toDF("c1")

  // Save as parquet with a single file
  val parquetPath = s"$outputPath/parquetV1"
  /*df.write
    .mode("overwrite")
    .option("compression", "snappy")
    //.option("maxRecordsPerFile", "1") // Ensure a single file is generated
    .parquet(parquetPath)*/

  // Load back as table
  spark.read.parquet(parquetPath)
    .createOrReplaceTempView("parquetV1Table")

  // Print some info
  println(s"Generated parquet table with $numRows rows at: $parquetPath")
  println("Sample of data:")
  spark.sql("SELECT * FROM parquetV1Table").show(5)
}

generateAndLoadParquetTable(1024*1024*100, "/localssd/hza214")


/*val numRows = 1024*1024*100
spark.range(numRows)
     .map(_ => 1)  // All values will be 1
     .toDF("c1")
     .createOrReplaceTempView("rowTable")
val startTime = System.nanoTime()
spark.sql("select c1 from rowTable")
     .repartition(48, new Column("c1"))
     .foreach(_ => ())
val endTime = System.nanoTime()
val durationSeconds = (endTime - startTime) / 1e9d
println(f"Shuffle operation took $durationSeconds%.3f seconds")x*/


val startTime = System.nanoTime()
spark.sql(s"select c1 from parquetV1Table")
     .repartition(48, new Column("c1"))
     .show()
val endTime = System.nanoTime()
val durationSeconds = (endTime - startTime) / 1e9d
println(f"Shuffle operation took $durationSeconds%.3f seconds")