package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query13
  *
  * select	 c_count, count(*) as custdist
  * from	 (
  *   select c_id, count(o_id)
  *   from customer left outer join orders on
  *   (c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id and o_carrier_id > 8)
  *   group by c_id
  * ) as c_orders (c_id, c_count)
  * group by c_count
  * order by custdist desc, c_count desc
  */
class Q13 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val customer = dfReader.options(getTableOptions("customer")).load()
    val orders = dfReader.options(getTableOptions("order")).load()

    val forders = orders.filter($"o_carrier_id" > 8)
    val c_orders = customer.join(forders, $"c_w_id" === forders("o_w_id") &&
      $"c_d_id" === forders("o_d_id") &&
      $"c_id" === forders("o_c_id"), "left_outer")
      .select($"c_id", $"o_id")
      .groupBy($"c_id")
      .agg(count("o_id").as("c_count"))

    c_orders
      .groupBy("c_count")
      .agg(count("c_count").as("custdist"))
      .orderBy($"custdist".desc, $"c_count".desc)

  }

}
