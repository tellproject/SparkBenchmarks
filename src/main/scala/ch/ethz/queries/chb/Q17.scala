package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query17
  *
  * select	sum(ol_amount) / 2.0 as avg_yearly
  * from	orderline,
  * (select i_id, avg(ol_quantity) as a
  * from item, orderline
  * where i_data like '%b' and ol_i_id = i_id
  * group by i_id) t
  * where ol_i_id = t.i_id and ol_quantity < t.a
  */
class Q17 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orderline = dfReader.options(getTableOptions("order-line")).load()
    val item = dfReader.options(getTableOptions("item")).load()

    val orderline1 = orderline
    val orderline2 = orderline

    val fitem = item.filter($"i_data".like("%d"))

    val t = fitem.join(orderline1, fitem("i_id") === orderline1("ol_i_id"))
      .select(fitem("i_id"), orderline1("ol_quantity"))
      .groupBy(fitem("i_id"))
      .agg(avg(orderline1("ol_quantity")).as("avg_qty"))

    orderline2
      .join(t, t("i_id") === $"ol_i_id" && $"ol_quantity" < t("avg_qty"))
      .agg(sum("ol_amount")./(2.0).as("avg_yearly"))

  }
}
