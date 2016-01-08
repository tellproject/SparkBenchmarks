package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.sql.functions.udf

/**
  * Ch Query19
  *
  * select	sum(ol_amount) as revenue
  * from	orderline, item
  * where
  *    ( ol_i_id = i_id
  *      and i_data like '%a'
  *      and ol_quantity >= 1
  *      and ol_quantity <= 10
  *      and i_price between 1 and 400000
  *      and ol_w_id in (1,2,3)
  *      ) or (
  *      ol_i_id = i_id
  *      and i_data like '%b'
  *      and ol_quantity >= 1
  *      and ol_quantity <= 10
  *      and i_price between 1 and 400000
  *      and ol_w_id in (1,2,4)
  *      ) or (
  *      ol_i_id = i_id
  *      and i_data like '%c'
  *      and ol_quantity >= 1
  *      and ol_quantity <= 10
  *      and i_price between 1 and 400000
  *      and ol_w_id in (1,5,3)
  *   )
  */
class Q19 extends BenchmarkQuery {

  val aa = udf { (x: String) => x.matches("a") }
  val bb = udf { (x: String) => x.matches("b") }
  val cc = udf { (x: String) => x.matches("c") }

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orderline = dfReader.options(getTableOptions("order-line")).load()
    val item = dfReader.options(getTableOptions("item")).load()

    val forderline = orderline.filter($"ol_quantity" >= 1 && $"ol_quantity" <= 10)
    val fitem = item.filter($"i_price" >= 100 && $"i_price" <= 4000000)

    forderline.join(fitem, $"ol_i_id" === $"i_id")
      .filter(
        (($"i_data".like("a")) && $"ol_w_id".isin(1, 2, 3)) ||
          (($"i_data".like("b")) && $"ol_w_id".isin(1, 2, 4)) ||
          (($"i_data".like("c")) && $"ol_w_id".isin(1, 5, 3)))
      .select($"ol_amount")
      .agg(sum($"ol_amount").as("revenue"))
  }
}
