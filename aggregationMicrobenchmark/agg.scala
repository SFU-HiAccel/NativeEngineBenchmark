import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

val spark = SparkSession.builder().appName("Run Queries").getOrCreate()
val df1 = spark.read.format("parquet").load("/mnt/glusterfs/users/hza214/db-benchmark/_data/G1_1e7_1e1_0_0.parquet")
df1.createOrReplaceTempView("df1")

val df2 = spark.read.format("parquet").load("/mnt/glusterfs/users/hza214/db-benchmark/_data/G1_1e7_1e2_0_0.parquet")
df2.createOrReplaceTempView("df2")

val df3 = spark.read.format("parquet").load("/mnt/glusterfs/users/hza214/db-benchmark/_data/G1_1e7_1e3_0_0.parquet")
df3.createOrReplaceTempView("df3")

val df4 = spark.read.format("parquet").load("/mnt/glusterfs/users/hza214/db-benchmark/_data/G1_1e7_1e4_0_0.parquet")
df4.createOrReplaceTempView("df4")

val df5 = spark.read.format("parquet").load("/mnt/glusterfs/users/hza214/db-benchmark/_data/G1_1e7_1e5_0_0.parquet")
df5.createOrReplaceTempView("df5")

val df6 = spark.read.format("parquet").load("/mnt/glusterfs/users/hza214/db-benchmark/_data/G1_1e7_1e6_0_0.parquet")
df6.createOrReplaceTempView("df6")

val df7 = spark.read.format("parquet").load("/mnt/glusterfs/users/hza214/db-benchmark/_data/G1_1e7_1e7_0_0.parquet")
df7.createOrReplaceTempView("df7")

val queries = Array(
  "select id1, count(v1) as v1 from df group by id1", 
  "select id1, sum(v1) as v1 from df group by id1", 
  //"select id1, id2, sum(v1) as v1 from df group by id1, id2", // 2
  //"select id3, sum(v1) as v1, mean(v3) as v3 from df group by id3",
  //"select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from df group by id4", //4
  //"select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from df group by id6",
  //"select id3, max(v1) - min(v2) as range_v1_v2 from df group by id3", // 7
  //"select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from df where v3 is not null) sub_query where order_v3 <= 2",
  //"select id2, id4, pow(corr(v1, v2), 2) as r2 from df group by id2, id4",
  //"select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from df group by id1, id2, id3, id4, id5, id6"
)

val tables = Array("df1","df2","df3","df4", "df5","df6","df7")

val writer = new PrintWriter(new java.io.File("/localhdd/hza214/BenchmarkPaperCode/agg/countVanilla.txt"))

for (table <- tables) {
  for (query <- queries) {
    val newQuery = query.replace("df", table)
    var totalDuration = 0.0

    writer.write(s"Query: $newQuery\n")

    for (i <- 1 to 3) {
      val startTime = System.nanoTime()
      spark.sql(newQuery).foreach(_ => ()) // Trigger the query execution
      val endTime = System.nanoTime()
      val duration = (endTime - startTime) / 1e9d
      totalDuration += duration
      writer.write(s"Run $i Duration: $duration seconds\n")
    }

    val avgDuration = totalDuration / 3
    writer.write(s"Average Duration: $avgDuration seconds\n\n")
  }
}

writer.close()

