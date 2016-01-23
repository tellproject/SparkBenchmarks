package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * TPC-H Query 20
  * Savvas Savvides <ssavvides@us.ibm.com>
  *
  */
class Q20 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val lineitem = dfReader.options(getTableOptions("lineitem")).load()
    val nation = dfReader.options(getTableOptions("nation", ("useSmallMemory" -> "true"))).load()
    val supplier = dfReader.options(getTableOptions("supplier")).load()
    val partsupp = dfReader.options(getTableOptions("partsupp")).load()
    val part = dfReader.options(getTableOptions("part")).load()

    val flineitem = lineitem.filter($"l_shipdate" >= referenceDate1994 && $"l_shipdate" < referenceDate1995)
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((sum($"l_quantity") * 0.5).as("sum_quantity"))

    val fnation = nation.filter($"n_name" === "CANADA")
    val nat_supp = supplier.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
      .join(fnation, $"s_nationkey" === fnation("n_nationkey"))

    part.filter($"p_name".startsWith("forest"))
      .select($"p_partkey").distinct
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(flineitem, $"ps_suppkey" === flineitem("l_suppkey") && $"ps_partkey" === flineitem("l_partkey"))
      .filter($"ps_availqty" > $"sum_quantity")
      .select($"ps_suppkey").distinct
      .join(nat_supp, $"ps_suppkey" === nat_supp("s_suppkey"))
      .select($"s_name", $"s_address")
      .sort($"s_name")

  }
}
