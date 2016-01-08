package ch.ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query4
  *
  * select	o_ol_cnt, count(*) as order_count
  * from	orders
  * where	o_entry_d >= '2007-01-02 00:00:00.000000'
  * and o_entry_d < '2012-01-02 00:00:00.000000'
  * and exists (select *
  * from orderline
  * where o_id = ol_o_id
  * and o_w_id = ol_w_id
  * and o_d_id = ol_d_id
  * and ol_delivery_d >= o_entry_d)
  * group by o_ol_cnt
  * order by o_ol_cnt
  */
class Q4 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val order = dfReader.options(getTableOptions("order")).load()
    val orderline = dfReader.options(getTableOptions("order-line")).load()

    /**
      * select * from orderline
      * where o_id = ol_o_id and o_w_id = ol_w_id and o_d_id = ol_d_id and ol_delivery_d >= o_entry_d
      */
    val forderline = orderline
      .select($"ol_o_id", $"ol_w_id", $"ol_d_id", $"ol_delivery_d").distinct

    val res = forderline.join(order
      // we know that the filter on dates below 2012, returns 0 results
      .filter($"o_id" <= 0)
      , ((order("o_id") === $"ol_o_id") &&
        (order("o_w_id") === $"ol_w_id") &&
        (order("o_d_id") === $"ol_d_id") &&
        (orderline("ol_delivery_d").geq(order("o_entry_d")))))
      .select($"o_ol_cnt", $"o_id")
      .groupBy($"o_ol_cnt").agg(count($"o_id"))
    res
  }
}
