package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query5
  *
  * select	 n_name,
  * sum(ol_amount) as revenue
  * from	 customer, orders, orderline, stock, supplier, nation, region
  * where	 c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
  * and ol_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_w_id = s_w_id and ol_i_id = s_i_id
  * and mod((s_w_id * s_i_id),10000) = su_suppkey
  * and ascii(substr(c_state,1,1)) = su_nationkey
  * and su_nationkey = n_nationkey
  * and n_regionkey = r_regionkey
  * and r_name = 'Europe'
  * and o_entry_d >= '2007-01-02 00:00:00.000000'
  * group by n_name
  * order by revenue desc
  */
class Q5 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val customer = dfReader.options(getTableOptions("customer")).load()
    val orders = dfReader.options(getTableOptions("order")).load()
    val orderline = dfReader.options(getTableOptions("order-line")).load()
    val region = dfReader.options(getTableOptions("region", ("useSmallMemory" -> "true"))).load()
    val nation = dfReader.options(getTableOptions("nation", ("useSmallMemory" -> "true"))).load()
    val supplier = dfReader.options(getTableOptions("supplier")).load()
    val stock = dfReader.options(getTableOptions("stock")).load()

    val forder = orders
      .filter(orders("o_entry_d").geq(20070102))
    val fregion = region
      .filter(region("r_name").eqNullSafe("Europe"))
    val part_res = customer.join(forder, ($"c_id" === forder("o_c_id")) &&
      ($"c_w_id" === forder("o_w_id")) &&
      ($"c_d_id" === forder("o_d_id")))
      .join(orderline, (orderline("ol_o_id") === forder("o_id")) &&
        (orderline("ol_w_id") === forder("o_w_id")) &&
        (orderline("ol_d_id") === forder("o_d_id")))
      .join(stock, (orderline("ol_w_id") === stock("s_w_id")) &&
        (orderline("ol_i_id") === stock("s_i_id")))

    val jsupp = supplier.join(nation, $"su_nationkey" === nation("n_nationkey"))
      .join(fregion, nation("n_regionkey") === region("r_regionkey"))

    val part_2 = part_res
      .join(jsupp, (part_res("s_w_id") * part_res("s_i_id") % 10000 === jsupp("su_suppkey")) &&
        (part_res("c_state").substr(1, 1).eq(jsupp("su_nationkey"))))

    part_2.groupBy(part_2("n_name"))
      .agg(sum($"ol_amount").as("revenue"))
      .orderBy("revenue")
      .select("n_name", "revenue")
  }
}
