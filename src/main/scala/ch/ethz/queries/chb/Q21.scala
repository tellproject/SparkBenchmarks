package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query21
  *
  * select	 su_name, count(*) as numwait
  * from	 supplier, orderline l1, orders, stock, nation
  * where	 ol_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id and l1.ol_delivery_d > o_entry_d
  * and ol_w_id = s_w_id and ol_i_id = s_i_id and mod((s_w_id * s_i_id),10000) = su_suppkey
  * and not exists (
  *    select * from orderline l2
  *    where l2.ol_o_id = l1.ol_o_id and l2.ol_w_id = l1.ol_w_id
  *    and l2.ol_d_id = l1.ol_d_id and l2.ol_delivery_d > l1.ol_delivery_d
  *    )
  * and su_nationkey = n_nationkey and n_name = 'Germany'
  * group by su_name
  * order by numwait desc, su_name
  */
class Q21 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orders = dfReader.options(getTableOptions("order")).load()
    val orderline = dfReader.options(getTableOptions("order-line")).load()
    val nation = dfReader.options(getTableOptions("nation", ("useSmallMemory" -> "true"))).load()
    val supplier = dfReader.options(getTableOptions("supplier")).load()
    val stock = dfReader.options(getTableOptions("stock")).load()

    val fnation = nation.filter($"n_name" === "Germany")
    val orderline1 = orderline
    val orderline2 = orderline

    val s_n = supplier.join(fnation, $"su_nationkey" === "n_nationkey")
    val s_s_n = s_n.join(stock, (($"s_w_id" * $"s_i_id") % 10000) === $"su_suppkey")

    orderline1.join(orderline2, ((orderline1("ol_o_id") !== orderline2("ol_o_id")) &&
      (orderline1("ol_w_id") !== orderline2("ol_w_id")) &&
      (orderline1("ol_d_id") !== orderline2("ol_d_id")) &&
      (orderline1("ol_delivery_d") > orderline2("ol_delivery_d"))))
      //      ol_w_id = s_w_id and ol_i_id = s_i_id
      .join(s_s_n,
      orderline1("ol_w_id") === $"s_w_id" &&
        orderline1("ol_i_id") === $"s_i_id")
      .join(orders,
        orderline1("ol_o_id") === $"o_id" &&
          orderline1("ol_w_id") === $"o_w_id" &&
          orderline1("ol_d_id") === $"o_d_id" &&
          orderline1("ol_delivery_d") > $"o_entry_d")
      .select($"su_name", $"o_id")
      .groupBy($"su_name")
      // todo is this the same? count(*) as numwait
      .agg(count($"o_id").as("numwait"))
      .orderBy($"numwait".desc, $"su_name")

  }
}
