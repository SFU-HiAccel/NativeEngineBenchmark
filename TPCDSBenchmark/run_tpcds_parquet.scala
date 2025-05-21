/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
var parquet_file_path = "/mnt/glusterfs/users/hza214/parquet_1T"

// File root path: file://, hdfs:// , s3 , ...
// e.g. hdfs://hostname:8020
var paq_file_root = "file://"
var gluten_root = "/localhdd/hza214/gluten"

var tpcds_queries_path = "/gluten-core/src/test/resources/slow_queries/"

var queries_no_decimal = "tpcds.queries.no-decimal"
var queries_original = "tpcds.queries.original"

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
val store_sales = spark.read.format("parquet").load(paq_file_root + parquet_file_path + "/store_sales")
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
store_sales.createOrReplaceTempView("store_sales")
time_dim.createOrReplaceTempView("time_dim")
warehouse.createOrReplaceTempView("warehouse")
web_page.createOrReplaceTempView("web_page")
web_returns.createOrReplaceTempView("web_returns")
web_sales.createOrReplaceTempView("web_sales")
web_site.createOrReplaceTempView("web_site")
date_dim.createOrReplaceTempView("date_dim")

//spark.sql("select  i_item_desc       ,w_warehouse_name       ,d1.d_week_seq       ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo       ,sum(case when p_promo_sk is not null then 1 else 0 end) promo       ,count(*) total_cnt from catalog_sales join inventory on (cs_item_sk = inv_item_sk) join warehouse on (w_warehouse_sk=inv_warehouse_sk) join item on (i_item_sk = cs_item_sk) join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk) join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk) join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk) join date_dim d2 on (inv_date_sk = d2.d_date_sk) join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk) left outer join promotion on (cs_promo_sk=p_promo_sk) left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number) where d1.d_week_seq = d2.d_week_seq   and inv_quantity_on_hand < cs_quantity    and d3.d_date > d1.d_date + 5   and hd_buy_potential = '1001-5000'   and d1.d_year = 2001   and cd_marital_status = 'M' group by i_item_desc,w_warehouse_name,d1.d_week_seq order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq  LIMIT 100 ;  ").show


def getListOfFiles(dir: String): List[File] = {
  val d = new File(dir)
  if (d.exists && d.isDirectory) {
    // You can run a specific query by using below line.
    // d.listFiles.filter(_.isFile).filter(_.getName().contains("17.sql")).toList
    d.listFiles.filter(_.isFile).toList
  } else {
    List[File]()
  }
}


val fileLists =
  getListOfFiles("/localhdd/hza214/gluten/gluten-core/src/test/resources/slow_queries")
val sorted = fileLists.sortBy {
  f => f.getName match {
    case name =>
      var str = name
      str = str.replaceFirst("a", ".1")
      str = str.replaceFirst("b", ".2")
      str = str.replaceFirst(".sql", "")
      str = str.replaceFirst("q", "")
      str.toDouble
  }}



def time[R](block: => R): Double = {
  val t0 = System.nanoTime()
  val result = block    // call-by-name
  val t1 = System.nanoTime()
  val elapsedTime = (t1 - t0) / 1e9
  elapsedTime
}


// Define a PrintWriter to write to a file
try {
  for (t <- sorted) {
    println(t)
    val fileContents = Source.fromFile(t).getLines.filter(!_.startsWith("--")).mkString(" ")
    println(fileContents)
    
    val fileName = t.toString.split("/").last
    val queryIdentifier = fileName.split("\\.")(0) // This will give you 'q1', 'q2', 'q23a', etc.
    
    val writer = new PrintWriter(s"/localhdd/hza214/gluten/tools/workload/tpcds/run_tpcds/parquet_query_times_$queryIdentifier.txt")
    try {
      val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val begin: LocalDateTime = LocalDateTime.now()
      writer.println(s"The begin time is: ${begin.format(formatter)}")
      val elapsedTime = time {
        spark.sql(fileContents).show
      }
      val end: LocalDateTime = LocalDateTime.now()
      // Write query time to file along with query details
      writer.println(t)
      writer.println(s"Query: $fileContents")
      writer.println(s"Elapsed time: $elapsedTime seconds\n")
      writer.println(s"The end time is: ${end.format(formatter)}")

    } catch {
      case e: Exception => 
        e.printStackTrace()
        println(s"Error processing file $t: ${e.getMessage}")
    } finally {
      writer.close()
    }
  }
} finally {
  // Any necessary final cleanup can go here
}
//spark.conf.set("spark.gluten.sql.enable.offloadtofpga", true)
//spark.conf.set("spark.gluten.sql2fpga.libpath", "/localhdd/hza215/gluten/SQL2FPGA/libsql2fpga.so")
