package ch.ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query10
  *
  * select	 c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
  * from	 customer, orders, orderline, nation
  * where	 c_id = o_c_id
  * and c_w_id = o_w_id and c_d_id = o_d_id and ol_w_id = o_w_id
  * and ol_d_id = o_d_id and ol_o_id = o_id and o_entry_d <= ol_delivery_d
  * and o_entry_d >= '2007-01-02 00:00:00.000000'
  * and n_nationkey = ascii(substr(c_state,1,1))
  * group by c_id, c_last, c_city, c_phone, n_name
  * order by revenue desc
  */
class Q10 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val customer = dfReader.options(getTableOptions("customer")).load()
    val orders = dfReader.options(getTableOptions("order")).load()
    val orderline = dfReader.options(getTableOptions("order-line")).load()
    val nation = dfReader.options(getTableOptions("nation", ("useSmallMemory" -> "true"))).load()

    val forders = orders.filter($"o_entry_d" >= 20070102)
    val c_n = customer.join(nation, $"c_state".substr(1, 1) === nation("n_nationkey"))
    val o_ol = forders.join(orderline, (orderline("ol_w_id") === $"o_w_id" &&
      orderline("ol_d_id") === $"o_d_id" &&
      orderline("ol_o_id") === $"o_id" &&
      orderline("ol_delivery_d") >= $"o_entry_d"))

    c_n.join(o_ol, ((c_n("c_id") === o_ol("o_c_id")) &&
      (c_n("c_w_id") === o_ol("o_w_id")) &&
      (c_n("c_d_id") === o_ol("o_d_id"))))
      //c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
      .select("c_id", "c_last", "c_city", "c_phone", "n_name", "ol_amount")
      .agg(sum($"ol_amount").as("revenue"))
      .orderBy($"revenue".desc)

  }
}