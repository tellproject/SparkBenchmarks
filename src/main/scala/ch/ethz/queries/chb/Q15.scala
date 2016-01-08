package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query15
  *
  * with	 revenue (supplier_no, total_revenue) as (
  * select	mod((s_w_id * s_i_id),10000) as supplier_no,
  * sum(ol_amount) as total_revenue
  * from	orderline, stock
  * where ol_i_id = s_i_id and ol_supply_w_id = s_w_id
  * and ol_delivery_d >= '2007-01-02 00:00:00.000000'
  * group by mod((s_w_id * s_i_id),10000))
  *
  * select	 su_suppkey, su_name, su_address, su_phone, total_revenue
  * from	 supplier, revenue
  * where	 su_suppkey = supplier_no
  * and total_revenue = (select max(total_revenue) from revenue)
  * order by su_suppkey
  */
class Q15 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orderline = dfReader.options(getTableOptions("order-line")).load()
    val supplier = dfReader.options(getTableOptions("supplier")).load()
    val stock = dfReader.options(getTableOptions("stock")).load()

    val forderline = orderline.filter($"ol_delivery_d" >= 20070102)

    val revenue = forderline.join(stock, ($"ol_i_id" === stock("s_i_id") && $"ol_supply_w_id" === stock("s_w_id")))
      .select((($"s_w_id" * $"s_i_id") % 10000).as("supplier_no"), $"ol_amount")
      .groupBy($"supplier_no")
      .agg(sum($"ol_amount").as("total_revenue"))
      .select($"total_revenue", $"supplier_no")

    val max_revenue = revenue.select($"total_revenue").agg(max($"total_revenue").as("total_revenue"))
    supplier.join(revenue, $"su_suppkey" === revenue("supplier_no"))
      //.filter(revenue("total_revenue") === max_revenue("total_revenue"))
      .join(max_revenue, revenue("total_revenue") === max_revenue("total_revenue"))
      .orderBy($"su_suppkey")

  }
}
