package ch.ethz.utils

import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

case class Customer(
                     c_custkey: Int,
                     c_name: String,
                     c_address: String,
                     c_nationkey: Int,
                     c_phone: String,
                     c_acctbal: Double,
                     c_mktsegment: String,
                     c_comment: String)

case class Lineitem(
                     l_orderkey: Int,
                     l_partkey: Int,
                     l_suppkey: Int,
                     l_linenumber: Int,
                     l_quantity: Double,
                     l_extendedprice: Double,
                     l_discount: Double,
                     l_tax: Double,
                     l_returnflag: String,
                     l_linestatus: String,
                     //                     l_shipdate: String,
                     //                     l_commitdate: String,
                     //                     l_receiptdate: String,
                     l_shipdate: Long,
                     l_commitdate: Long,
                     l_receiptdate: Long,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String)

case class Nation(
                   n_nationkey: Int,
                   n_name: String,
                   n_regionkey: Int,
                   n_comment: String)

case class Order(
                  o_orderkey: Int,
                  o_custkey: Int,
                  o_orderstatus: String,
                  o_totalprice: Double,
                  //                  o_orderdate: String,
                  o_orderdate: Long,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: Int,
                  o_comment: String)

case class Part(
                 p_partkey: Int,
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Int,
                 p_container: String,
                 p_retailprice: Double,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Int,
                     ps_suppkey: Int,
                     ps_availqty: Int,
                     ps_supplycost: Double,
                     ps_comment: String)

case class Region(
                   r_regionkey: Int,
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Int,
                     s_name: String,
                     s_address: String,
                     s_nationkey: Int,
                     s_phone: String,
                     s_acctbal: Double,
                     s_comment: String)

/**
  * CsvParquetConverter
  */
object TsvParquetConverter {

  var inputDir: String = ""
  var outputPath: String = ""
  var masterIp: String = "local"

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: <master> <inputDir> <outputDir>")
      println("\t\t <master> master ip")
      println("\t\t <inputDir> path to csv files")
      println("\t\t <outputDir> output dir for parquet files")
    }

    masterIp = args(0)
    inputDir = args(1)
    outputPath = args(2)

    if (Files.exists(Paths.get(inputDir))) {
      val conf = new SparkConf().setMaster(masterIp).setAppName("Converter")
      val sc = new SparkContext(conf)
      val sqlCxt = new SQLContext(sc)
      import sqlCxt.implicits._

      val customer = sc.textFile(inputDir + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
      val lineitem = sc.textFile(inputDir + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, DateConverter.dateToLong(p(10).trim), DateConverter.dateToLong(p(11).trim), DateConverter.dateToLong(p(12).trim), p(13).trim, p(14).trim, p(15).trim)).toDF()
      val nation = sc.textFile(inputDir + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF()
      val region = sc.textFile(inputDir + "/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toInt, p(1).trim, p(1).trim)).toDF()
      val order = sc.textFile(inputDir + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, DateConverter.dateToLong(p(4).trim), p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF()
      val part = sc.textFile(inputDir + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
      val partsupp = sc.textFile(inputDir + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF()
      val supplier = sc.textFile(inputDir + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

      if (Files.exists(Paths.get(outputPath))) {
        customer.write.parquet(outputPath + "/customer.parquet")
        lineitem.write.parquet(outputPath + "/lineitem.parquet")
        nation.write.parquet(outputPath + "/nation.parquet")
        region.write.parquet(outputPath + "/region.parquet")
        order.write.parquet(outputPath + "/orders.parquet")
        part.write.parquet(outputPath + "/part.parquet")
        partsupp.write.parquet(outputPath + "/partsupp.parquet")
        supplier.write.parquet(outputPath + "/supplier.parquet")
      }
      println("Doing sanity check")
      val dfReader = sqlCxt.read.format("parquet")
      val cc = dfReader.option("path", outputPath + "/nation.parquet").load()
      cc.show(10)
    } else {
      println(s"Input path does not exist ${inputDir}")
    }

  }
}
