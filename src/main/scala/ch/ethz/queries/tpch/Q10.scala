package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

import scala.collection.mutable

/**
  * TPC-H Query 10
  * Savvas Savvides <ssavvides@us.ibm.com>
  *
  */
class Q10 extends BenchmarkQuery {


  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val customer = dfReader.options(getTableOptions("customer")).load()
    val order = dfReader.options(getTableOptions("orders")).load()
    val lineitem = dfReader.options(getTableOptions("lineitem")).load()
    val nation = dfReader.options(getTableOptions("nation", ("useSmallMemory" -> "true"))).load()

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val flineitem = lineitem.filter($"l_returnflag" === "R")

    order.filter($"o_orderdate" < referenceDate1994 && $"o_orderdate" >= referenceDate1993)
      .join(customer, $"o_custkey" === customer("c_custkey"))
      .join(nation, $"c_nationkey" === nation("n_nationkey"))
      .join(flineitem, $"o_orderkey" === flineitem("l_orderkey"))
      .select($"c_custkey", $"c_name",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment")
      .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc)
      .limit(20)

  }

}
