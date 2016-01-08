package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * TPC-H Query 13
  * Savvas Savvides <ssavvides@us.ibm.com>
  *
  */
class Q13 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    //    val region = dfReader.option("table", "region").option("useSmallMemory", "true").load()
    //    val nation = dfReader.option("table", "nation").option("useSmallMemory", "true").load()
    val customer = dfReader.option("table", "customer").load()
    val order = dfReader.option("table", "order").load()
    //    val lineitem = dfReader.option("table", "lineitem").load()
    //    val supplier = dfReader.option("table", "supplier").load()
    //    val partsupp = dfReader.option("table", "partsupp").load()
    //    val part = dfReader.option("table", "part").load()

    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)

  }

}
