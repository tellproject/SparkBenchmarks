package ch.ethz

import ch.ethz.tell.TellContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object Application {

  def test(): Unit = {
    val sc = new SparkContext(new SparkConf())
    val context = new ch.ethz.tell.TellContext(sc)
    context.startTransaction()
    import context.implicits._

    val t = (name: String) => context.read.format("ch.ethz.tell").options(Map("table" -> name, "numPartitions" -> sc.getConf.get("spark.sql.tell.numPartitions"), "useSmallMemory" -> "false")).load()
    val tP = (name: String) => context.read.format("parquet").options(Map("path" -> s"/mnt/SG/braunl-tpch-data/parquet/1/${name}.parquet")).load()

    val nation = t("nation")
    val region = t("region")
    val supplier = t("supplier")
    val partsupp = t("partsupp")
    val part = t("part")

    val nationP = tP("nation")
    val regionP = tP("region")
    val supplierP = tP("supplier")
    val partsuppP = tP("partsupp")
    val partP = tP("part")

    val europe = region.filter($"r_name" === "EUROPE").join(nation, $"r_regionkey" === nation("n_regionkey")).join(supplier, $"n_nationkey" === supplier("s_nationkey")).join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    val europeP = regionP.filter($"r_name" === "EUROPE").join(nationP, $"r_regionkey" === nationP("n_regionkey")).join(supplierP, $"n_nationkey" === supplierP("s_nationkey")).join(partsuppP, supplierP("s_suppkey") === partsuppP("ps_suppkey"))

    // Query 18
    val customer = t("customer")
    val order    = t("orders")
    val lineitem = t("lineitem")

    val customerP = tP("customer")
    val orderP    = tP("orders")
    val lineitemP = tP("lineitem")

    lineitem.groupBy($"l_orderkey").agg(sum($"l_quantity").as("sum_quantity")).filter($"sum_quantity" > 300).select($"l_orderkey".as("key"), $"sum_quantity").join(order, order("o_orderkey") === $"key").join(lineitem, $"o_orderkey" === lineitem("l_orderkey")).join(customer, customer("c_custkey") === $"o_custkey").select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice").groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice").agg(sum("l_quantity")).sort($"o_totalprice".desc, $"o_orderdate").limit(100)
    lineitemP.groupBy($"l_orderkey").agg(sum($"l_quantity").as("sum_quantity")).filter($"sum_quantity" > 300).select($"l_orderkey".as("key"), $"sum_quantity").join(orderP, orderP("o_orderkey") === $"key").join(lineitemP, $"o_orderkey" === lineitemP("l_orderkey")).join(customerP, customerP("c_custkey") === $"o_custkey").select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice").groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice").agg(sum("l_quantity")).sort($"o_totalprice".desc, $"o_orderdate").limit(100)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val context = new TellContext(sc)
    context.startTransaction()

    val data = context.read.format("tell").options(Map(
      "table" -> "testTable",
      "numPartitions" -> "8"
    )).load()
    data.filter(data("number") >= 14).select("text2", "number").show(250)

    context.commitTransaction()
  }
}
