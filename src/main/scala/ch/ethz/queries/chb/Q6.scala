package ch.ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Query6
  *
  * select	sum(ol_amount) as revenue
  * from	orderline
  * where	ol_delivery_d >= '1999-01-01 00:00:00.000000'
  * and ol_delivery_d < '2020-01-01 00:00:00.000000'
  * and ol_quantity between 1 and 100000
  */
class Q6 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orderline = dfReader.option("table", "order-line").load()

    orderline
      .filter($"ol_delivery_d" >= 19990101)
      .filter($"ol_delivery_d" < 20200101)
      .filter($"ol_quantity" >= 1)
      .filter($"ol_quantity" <= 10000)
      .agg(sum($"ol_amount"))
  }
}
