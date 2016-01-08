package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query18
  *
  * select	 c_last, c_id o_id, o_entry_d, o_ol_cnt, sum(ol_amount)
  * from customer, orders, orderline
  * where c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
  * and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
  * group by o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt
  * having sum(ol_amount) > 200
  * order by sum(ol_amount) desc, o_entry_d
  */
class Q18 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._


    val customer = dfReader.options(getTableOptions("customer")).load()
    val orders = dfReader.options(getTableOptions("order")).load()
    val orderline = dfReader.options(getTableOptions("order-line")).load()

    customer.join(orders, $"c_id" === orders("o_c_id") &&
      $"c_w_id" === orders("o_w_id") &&
      $"c_d_id" === orders("o_d_id"))
      .join(orderline, orderline("ol_w_id") === $"o_w_id" &&
        orderline("ol_d_id") === $"o_d_id" &&
        orderline("ol_o_id") === $"o_id")
      .select("c_last", "c_id", "o_id", "o_entry_d", "o_ol_cnt", "ol_amount")
      .agg(sum($"ol_amount").as("tot_amount"), first($"o_entry_d").as("o_entry_d"))
      .filter($"tot_amount" > 20000)
      .orderBy($"tot_amount".desc, $"o_entry_d")

  }
}
