package ch.ethz.queries.tpch

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * TPC-H Query 19
  * Savvas Savvides <ssavvides@us.ibm.com>
  *
  */
class Q19 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val lineitem = dfReader.options(getTableOptions("lineitem")).load()
    val part = dfReader.options(getTableOptions("part")).load()

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // project part and lineitem first?
    part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#12") &&
          $"p_container".isin("SM CASE", "SM BOX", "SM PACK", "SM PKG") &&
          $"l_quantity" >= 1 && $"l_quantity" <= 11 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#23") &&
            $"p_container".isin("MED BAG", "MED BOX", "MED PKG", "MED PACK") &&
            $"l_quantity" >= 10 && $"l_quantity" <= 20 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
          (($"p_brand" === "Brand#34") &&
            $"p_container".isin("LG CASE", "LG BOX", "LG PACK", "LG PKG") &&
            $"l_quantity" >= 20 && $"l_quantity" <= 30 &&
            $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))

  }
}
