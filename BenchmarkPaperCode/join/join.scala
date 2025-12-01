import org.apache.spark.sql.SparkSession
import scala.util.Random
import java.io.PrintWriter
// Import implicits for DataFrame operations
import spark.implicits._

// Function to generate a sequence of random data tuples (key, value) with 20% selectivity
def generateTable(size: Int, keyRange: Int): Seq[(Int, Int)] = {
  // keyRange controls the number of distinct keys to achieve 20% selectivity
  (1 to size).map(_ => (Random.nextInt(keyRange), Random.nextInt(1000)))
}

// Hash table sizes in bytes (50% fill rate, so adjust table size accordingly)
val hashTableSizes = Seq(32 * 1024*1024)

// Size of each tuple in bytes: 8 bytes (4 for key + 4 for value)
val tupleSize = 8

// Output directory for Parquet files
val outputDir = "output/"

val probeTableSize = 64 * 1024 * 1024 // Number of tuples
// To achieve 20% selectivity, we'll use a keyRange that is 5x smaller than the build table size
val probeKeyRange = probeTableSize / 5
val probeTable = spark.sparkContext.parallelize(generateTable(probeTableSize, probeKeyRange)).toDF("customer_id", "v")
probeTable.write.mode("overwrite").parquet(s"${outputDir}probe_table.parquet")

val writer = new PrintWriter(new java.io.File("./hashTable.txt"))

// Step 2: Iterate through hash table sizes, generate build tables, save as Parquet, and perform joins
hashTableSizes.foreach { hashTableSize =>
  // Calculate number of tuples for the build table to achieve desired hash table size
  val numTuples = (hashTableSize / (tupleSize * 2)).toInt // 50% fill rate
  
  // Use the same keyRange as probe table to maintain 20% selectivity
  val buildTable = spark.sparkContext.parallelize(generateTable(numTuples, probeKeyRange)).toDF("customer_id", "v")
  val buildPath = s"${outputDir}build_table_${hashTableSize / 1024}KB.parquet"
  buildTable.write.mode("overwrite").parquet(buildPath)

  // Step 3: Read the build and probe tables from Parquet
  val buildTableParquet = spark.read.parquet(buildPath)
  val probeTableParquet = spark.read.parquet(s"${outputDir}probe_table.parquet")

  // Step 4: Register tables as views for SQL queries
  buildTableParquet.createOrReplaceTempView("customer")
  probeTableParquet.createOrReplaceTempView("store_sales")

  // Step 5: Perform the join query
  val result = spark.sql("""
SELECT * FROM customer , store_sales WHERE
customer.customer_id =
store_sales.customer_id ;
  """)

  // Show the result (or save it for analysis)
  writer.write(s"Hash Table Size: ${hashTableSize / 1024} KB")
  var totalDuration = 0.0

  for (i <- 1 to 3) {
        spark.sqlContext.clearCache()
        val startTime = System.nanoTime()
        result.foreach(_ => ())
        val endTime = System.nanoTime()
        val duration = (endTime - startTime) / 1e9d
        totalDuration += duration
        writer.write(s"Run $i Duration: $duration seconds\n")
   }
   val avgDuration = totalDuration / 3
   writer.write(s"Average Duration: $avgDuration seconds\n\n")
}
writer.close()
// Stop the Spark session

//./spark-shell --conf spark.sql.adaptive.enabled=true --conf spark.sql.codegen.wholeStage=true --conf spark.plugins=org.apache.gluten.GlutenPlugin --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=20g --conf spark.executorEnv.LD_PRELOAD=/localhdd/hza214/gluten/cpp-ch/build/utils/extern-local-engine/libch.so --conf spark.gluten.sql.columnar.libpath=/localhdd/hza214/gluten/cpp-ch/build/utils/extern-local-engine/libch.so --conf spark.gluten.sql.columnar.iterator=true --conf spark.gluten.sql.columnar.loadarrow=false --conf spark.gluten.sql.columnar.hashagg.enablefinal=true --conf spark.gluten.sql.enable.native.validation=false --conf spark.gluten.sql.columnar.forceShuffledHashJoin=true --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager --executor-cores 4 --conf spark.local.dir=/localssd/hza214 --conf spark.default.parallelism=48 --conf spark.sql.shuffle.partitions=48 --conf spark.driver.memoryOverhead=4g --conf spark.executor.memory=16g --conf spark.executor.memoryOverhead=4g --driver-memory 40g