package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 1
 * Savvas Savvides <ssavvides@us.ibm.com>
 *
 */
class Q1 extends BenchmarkQuery {

  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val lineitem = dfReader.options(getTableOptions("lineitem")).load()

    lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
        sum(decrease($"l_extendedprice", $"l_discount")),
        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")

  }
}
