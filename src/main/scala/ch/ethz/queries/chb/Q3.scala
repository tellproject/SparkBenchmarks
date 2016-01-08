package ch.ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query3
  *
  * select ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) as revenue, o_entry_d
  * from customer, neworder, orders, orderline
  * where c_state like 'A%' and c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
  * and no_w_id = o_w_id and no_d_id = o_d_id and no_o_id = o_id
  * and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
  * and o_entry_d > '2007-01-02 00:00:00.000000'
  * group by ol_o_id, ol_w_id, ol_d_id, o_entry_d
  * order by revenue desc, o_entry_d
  */
class Q3 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orders = dfReader.option("table", "order").load()
    val customer = dfReader.option("table", "customer").load()
    val orderline = dfReader.option("table", "order-line").load()
    val new_order = dfReader.option("table", "new-order").load()

    customer
      .filter(customer("c_state").like("A%"))
      .join(orders, (($"c_id" === orders("o_c_id")) &&
        ($"c_w_id" === orders("o_w_id")) &&
        ($"c_d_id" === orders("o_d_id"))))
      .join(new_order, ($"o_w_id" === new_order("no_w_id")) &&
        ($"o_d_id" === new_order("no_d_id")) &&
        ($"o_id" === new_order("no_o_id")))
      .join(orderline, ($"o_w_id" === orderline("ol_w_id")) &&
        ($"o_d_id" === orderline("ol_d_id")) &&
        ($"o_id" === orderline("ol_o_id")))
      .filter(orders("o_entry_d") > 20070102)
      .groupBy(orderline("ol_o_id"), orderline("ol_w_id"), orderline("ol_d_id"), orders("o_entry_d"))
      .agg(sum($"ol_amount").as("revenue"))
      .select(orderline("ol_o_id"), orderline("ol_w_id"), orderline("ol_d_id"), orders("o_entry_d"))
      .orderBy($"revenue".desc, orders("o_entry_d"))

  }
}
