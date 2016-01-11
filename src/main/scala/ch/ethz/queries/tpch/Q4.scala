package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * TPC-H Query 4
  * Savvas Savvides <ssavvides@us.ibm.com>
  *
  */
class Q4 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val order = dfReader.options(getTableOptions("orders")).load()
    val lineitem = dfReader.options(getTableOptions("lineitem")).load()

    val forders = order.filter($"o_orderdate" >= referenceDate19930701 && $"o_orderdate" < referenceDate1993)
    val flineitems = lineitem.filter($"l_commitdate" < $"l_receiptdate")
      .select($"l_orderkey")
      .distinct

    flineitems.join(forders, $"l_orderkey" === forders("o_orderkey"))
      .groupBy($"o_orderpriority")
      .agg(count($"o_orderpriority"))
      .sort($"o_orderpriority")

  }

}
