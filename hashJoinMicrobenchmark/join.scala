// 

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

// 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024,
// Target hash table sizes in bytes
val targetHashTableSizes = Seq( 64 * 1024 * 1024)

// Hash table parameters
val hashTableFillRate = 0.75 // 50% fill rate
val hashTableEntryOverhead = 16 // Estimated overhead per hash table entry in bytes (pointer + metadata)

// Build table parameters
val tupleSize = 8 // Size of each tuple in bytes: 8 bytes (4 for key + 4 for value)

// Function to calculate build table size needed for target hash table size
def calculateBuildTableSize(targetHashTableSize: Int): Int = {
  // Each hash table entry consists of:
  // - The actual data (8 bytes per tuple)
  // - Hash table overhead (estimated 16 bytes per entry for pointers, metadata, etc.)
  val bytesPerHashEntry = tupleSize + hashTableEntryOverhead
  
  // Calculate number of entries that can fit in the hash table at the given fill rate
  val maxEntries = (targetHashTableSize * hashTableFillRate / bytesPerHashEntry).toInt
  
  // Build table size is just the number of tuples times tuple size
  val buildTableSize = maxEntries * tupleSize
  
  buildTableSize
}

// Function to calculate number of tuples for a given build table size
def calculateNumTuples(buildTableSize: Int): Int = {
  buildTableSize / tupleSize
}

// Output directory for Parquet files
val outputDir = "output/"

val probeTableSize = 64 * 1024 * 1024 // Number of tuples
// To achieve 20% selectivity, we'll use a keyRange that is 5x smaller than the build table size
val probeKeyRange = probeTableSize / 5
val probeTable = spark.sparkContext.parallelize(generateTable(probeTableSize, probeKeyRange)).toDF("customer_id", "v")
probeTable.write.mode("overwrite").parquet(s"${outputDir}probe_table.parquet")

val writer = new PrintWriter(new java.io.File("./hashTable.txt"))


// Step 2: Iterate through target hash table sizes
targetHashTableSizes.foreach { targetHashTableSize =>
  // Calculate the exact build table size needed
  val buildTableSize = calculateBuildTableSize(targetHashTableSize)
  val numTuples = calculateNumTuples(buildTableSize)
  
  // Calculate actual hash table size that will be created
  val actualHashTableSize = (numTuples * (tupleSize + hashTableEntryOverhead) / hashTableFillRate).toInt
  
  // // Use the same keyRange as probe table to maintain 20% selectivity
  val buildTable = spark.sparkContext.parallelize(generateTable(numTuples, probeKeyRange)).toDF("customer_id", "v")
  val buildPath = s"${outputDir}build_table_${targetHashTableSize / 1024}KB_target.parquet"
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

  // Print detailed sizing information
  writer.write("=" * 70 + "\n")
  writer.write(s"TARGET HASH TABLE SIZE: ${targetHashTableSize / 1024} KB (${targetHashTableSize} bytes)\n")
  writer.write(s"Build Table Tuples: ${numTuples}\n")
  writer.write(s"Build Table Size: ${buildTableSize / 1024} KB (${buildTableSize} bytes)\n")
  writer.write(s"Estimated Actual Hash Table Size: ${actualHashTableSize / 1024} KB (${actualHashTableSize} bytes)\n")
  writer.write(s"Size Difference: ${(actualHashTableSize - targetHashTableSize)} bytes\n")
  writer.write(s"Fill Rate: ${(numTuples * (tupleSize + hashTableEntryOverhead).toDouble / actualHashTableSize * 100).toInt}%\n")
  
  // Console output for immediate feedback
  println(f"Target: ${targetHashTableSize/1024}%4d KB | Build Table: ${numTuples}%8d tuples (${buildTableSize/1024}%4d KB) | Est. Hash Table: ${actualHashTableSize/1024}%4d KB")

  var totalDuration = 0.0

  for (i <- 1 to 1) {
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

// Print sizing relationship summary
writer.write("=" * 70 + "\n")
writer.write("BUILD TABLE SIZE TO HASH TABLE SIZE RELATIONSHIP:\n")
writer.write("Hash Table Size = (Build Table Tuples × (Tuple Size + Overhead)) ÷ Fill Rate\n")
writer.write(s"Where: Tuple Size = ${tupleSize} bytes, Overhead = ${hashTableEntryOverhead} bytes, Fill Rate = ${hashTableFillRate}\n")
writer.write("Build Table Size = Target Hash Table Size × Fill Rate × Tuple Size ÷ (Tuple Size + Overhead)\n\n")

writer.write("BENCHMARK COMPLETED\n")
writer.write(s"Total configurations tested: ${targetHashTableSizes.length}\n")
writer.close()

println("Benchmark completed. Results saved to hashTable.txt")

// Stop the Spark session