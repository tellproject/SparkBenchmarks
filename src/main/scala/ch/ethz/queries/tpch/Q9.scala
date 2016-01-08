package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * TPC-H Query 9
  * Savvas Savvides <ssavvides@us.ibm.com>
  *
  */
class Q9 extends BenchmarkQuery {

  val getYear = udf { (x: String) => x.substring(0, 4) }
  val getAmount = udf { (x: Double, y: Double, v: Double, w: Double) => val r = x * (1 - y) - (v * w); r }

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val nation = dfReader.option("table", "nation").option("useSmallMemory", "true").load()
    val order = dfReader.option("table", "order").load()
    val lineitem = dfReader.option("table", "lineitem").load()
    val supplier = dfReader.option("table", "supplier").load()
    val partsupp = dfReader.option("table", "partsupp").load()
    val part = dfReader.option("table", "part").load()

    val linePart = part.filter($"p_name".contains("green"))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year")
        ,getAmount($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"))
      .sort($"n_name", $"o_year".desc)

  }

}
