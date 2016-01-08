package ch.ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query12
  *
  * select
  *   o_ol_cnt,
  *   sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
  *   sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
  * from	 orders, orderline
  * where	 ol_w_id = o_w_id
  *   and ol_d_id = o_d_id and ol_o_id = o_id
  *   and o_entry_d <= ol_delivery_d and ol_delivery_d < '2020-01-01 00:00:00.000000'
  * group by o_ol_cnt
  * order by o_ol_cnt
  */
class Q12 extends BenchmarkQuery {

  val high_line_count = udf { (x: Int) => if (x == 1 || x == 2) 1 else 0 }
  val low_line_count = udf { (x: Int) => if (x != 1 && x != 2) 1 else 0 }

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import sqlCxt.implicits._

    val orders = dfReader.option("table", "order").load()
    val orderline = dfReader.option("table", "order-line").load()

    val forderline = orderline.filter($"ol_delivery_d" < 20200101)
    orders.join(forderline, forderline("ol_w_id") === $"o_w_id" &&
      forderline("ol_d_id") === $"o_d_id" &&
      forderline("ol_o_id") === $"o_id" &&
      $"o_entry_d" <= forderline("ol_delivery_d"))
      .select($"o_ol_cnt", $"o_carrier_id")
      .groupBy($"o_ol_cnt")
      .agg(sum(high_line_count($"o_carrier_id")), sum(low_line_count($"o_carrier_id")))
      .orderBy($"o_ol_cnt")

  }
}
