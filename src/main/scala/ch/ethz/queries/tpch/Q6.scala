package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * TPC-H Query 6
  * Savvas Savvides <ssavvides@us.ibm.com>
  *
  */
class Q6 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val lineitem = dfReader.options(getTableOptions("lineitem")).load()

    lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01"
      && $"l_discount" >= 0.05 && $"l_discount" <= 0.07
      && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))

  }
}
