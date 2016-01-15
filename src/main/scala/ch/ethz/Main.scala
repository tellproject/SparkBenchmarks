package ch.ethz

import ch.ethz.tell.TellContext
import org.apache.spark.{SparkConf, SparkContext}

object Application {

  def test(): Unit = {
    val sc = new SparkContext(new SparkConf())
    val context = new TellContext(sc)
    context.startTransaction()
    import context.implicits._

    val t = (name: String, small: Boolean) => context.read.format("ch.ethz.tell").options(Map("table" -> name, "numPartitions" -> "8", "useSmallMemory" -> (if (small) "true" else "false"))).load()
    val tP = (name: String) => context.read.format("parquet").options(Map("path" -> s"/mnt/SG/braunl-tpch-data/parquet/0.1/${name}.parquet")).load()

    val nation = t("nation", true)
    val region = t("region", true)
    val supplier = t("supplier", false)
    val partsupp = t("partsupp", false)
    val part = t("part", false)

    val nationP = tP("nation")
    val regionP = tP("region")
    val supplierP = tP("supplier")
    val partsuppP = tP("partsupp")
    val partP = tP("part")

    val europe = region.filter($"r_name" === "EUROPE").join(nation, $"r_regionkey" === nation("n_regionkey")).join(supplier, $"n_nationkey" === supplier("s_nationkey")).join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    val europeP = regionP.filter($"r_name" === "EUROPE").join(nationP, $"r_regionkey" === nationP("n_regionkey")).join(supplierP, $"n_nationkey" === supplierP("s_nationkey")).join(partsuppP, supplierP("s_suppkey") === partsuppP("ps_suppkey"))
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
