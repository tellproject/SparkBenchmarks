package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * TPC-H Query 16
  * Savvas Savvides <ssavvides@us.ibm.com>
  *
  */
class Q16 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val supplier = dfReader.options(getTableOptions("supplier")).load()
    val partsupp = dfReader.options(getTableOptions("partsupp")).load()
    val part = dfReader.options(getTableOptions("part")).load()

    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }

    val fparts = part.filter(($"p_brand" !== "Brand#45") && !($"p_type".startsWith("MEDIUM POLISHED")) &&
      $"p_size".isin(49, 14, 23, 45, 19, 3, 36, 9))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")

  }
}
