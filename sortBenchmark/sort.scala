import java.io.{File, FileOutputStream, PrintStream}

import org.apache.spark.sql.execution.debug._
import scala.io.Source
import java.io.File
import java.io.PrintWriter
import java.util.Arrays
import sys.process._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Configurations:
var parquet_file_path = "/mnt/glusterfs/users/hza214/parquet_100_partition"

// File root path: file://, hdfs:// , s3 , ...
// e.g. hdfs://hostname:8020
var paq_file_root = "file://"
var gluten_root = "/localhdd/hza214/gluten"

var tpcds_queries_path = "/gluten-core/src/test/resources/slow_queries/"

var queries_no_decimal = "tpcds.queries.no-decimal"
var queries_original = "tpcds.queries.original"
val store_sales = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/store_sales")
store_sales.createOrReplaceTempView("store_sales")


val catalog_sales = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/catalog_sales")
val catalog_page = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/catalog_page")
val catalog_returns = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/catalog_returns")
val customer = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/customer")
val customer_address = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/customer_address")
val customer_demographics = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/customer_demographics")
val date_dim = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/date_dim")
val household_demographics = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/household_demographics")
val income_band = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/income_band")
val inventory = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/inventory")
val item = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/item")
val promotion = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/promotion")
val reason = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/reason")
val ship_mode = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/ship_mode")
val store = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/store")
val store_returns = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/store_returns")
val time_dim = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/time_dim")
val warehouse = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/warehouse")
val web_page = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/web_page")
val web_returns = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/web_returns")
val web_sales = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/web_sales")
val web_site = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/web_site")
val call_center = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/call_center")

// Create parquet based TPC-DS Table View.
call_center.createOrReplaceTempView("call_center")
catalog_page.createOrReplaceTempView("catalog_page")
catalog_returns.createOrReplaceTempView("catalog_returns")
catalog_sales.createOrReplaceTempView("catalog_sales")
customer.createOrReplaceTempView("customer")
customer_address.createOrReplaceTempView("customer_address")
customer_demographics.createOrReplaceTempView("customer_demographics")
household_demographics.createOrReplaceTempView("household_demographics")
income_band.createOrReplaceTempView("income_band")
inventory.createOrReplaceTempView("inventory")
item.createOrReplaceTempView("item")
promotion.createOrReplaceTempView("promotion")
reason.createOrReplaceTempView("reason")
ship_mode.createOrReplaceTempView("ship_mode")
store.createOrReplaceTempView("store")
store_returns.createOrReplaceTempView("store_returns")
time_dim.createOrReplaceTempView("time_dim")
warehouse.createOrReplaceTempView("warehouse")
web_page.createOrReplaceTempView("web_page")
web_returns.createOrReplaceTempView("web_returns")
web_sales.createOrReplaceTempView("web_sales")
web_site.createOrReplaceTempView("web_site")
date_dim.createOrReplaceTempView("date_dim")

// Define the queries
val queries = Map(
  "Variation 1: Low-cardinality sort" ->
    """SELECT ss_promo_sk, ss_net_paid FROM store_sales ORDER BY ss_promo_sk""",
  "Variation 2: Decimal sort DESC" ->
    """SELECT ss_sales_price, ss_quantity FROM store_sales ORDER BY ss_sales_price DESC""",
  "Variation 3: Multi-column sort" ->
    """SELECT ss_store_sk, ss_sold_date_sk, ss_net_paid FROM store_sales 
       ORDER BY ss_store_sk ASC, ss_sold_date_sk DESC""",
  "Variation 4: Top-N with LIMIT" ->
    """SELECT ss_net_profit, ss_ticket_number FROM store_sales 
       ORDER BY ss_net_profit DESC LIMIT 1000""",
  "Variation 5: High-cardinality sort" ->
    """SELECT ss_ticket_number, ss_net_paid FROM store_sales ORDER BY ss_ticket_number"""
)

// Measure time for each query
queries.foreach { case (name, sql) =>
  println(s"Running: $name")
  val startTime = System.currentTimeMillis()
  try {
    val df = spark.sql(sql)
    val rowCount = df.count()  // Forces execution of the sort (without collecting all data)
    println(s"Query completed. Row count: $rowCount")
  } catch {
    case e: Exception => println(s"Error: ${e.getMessage}")
  }
  val endTime = System.currentTimeMillis()
  val durationSeconds = (endTime - startTime) / 1000.0
  println(s"Time taken: $durationSeconds seconds\n")
}