package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.sql.functions.udf

/**
  * Ch Query14
  *
  * select
  * 100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1+sum(ol_amount)) as promo_revenue
  * from	orderline, item
  * where	ol_i_id = i_id and ol_delivery_d >= '2007-01-02 00:00:00.000000'
  * and ol_delivery_d < '2020-01-02 00:00:00.000000'
  */
class Q14 extends BenchmarkQuery {

  val promo = udf { (x: String, y: Double) => if (x.startsWith("PR")) y else 0 }

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orderline = dfReader.option("table", "order-line").load()
    val item = dfReader.option("table", "item").load()

    val forderline = orderline.filter($"ol_delivery_d" >= 20070102 && $"ol_delivery_d" < 20200102)
    forderline.join(item, $"ol_i_id" === item("i_id"))
      .agg(sum(promo($"i_data", $"ol_amount")) * 100 / (sum($"ol_amount").+(1))).as("promo_revenue")

  }
}
