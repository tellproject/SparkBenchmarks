package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query20
  *
  * select	 su_name, su_address
  * from supplier, nation
  * where su_suppkey in (
  *    select  mod(s_i_id * s_w_id, 10000)
  *    from stock, orderline
  *    where s_i_id in (
  *       select i_id
  *       from item
  *       where i_data like 'co%'
  *    )
  *    and ol_i_id=s_i_id and ol_delivery_d > '2010-05-23 12:00:00'
  *    group by s_i_id, s_w_id, s_quantity
  *    having 2*s_quantity > sum(ol_quantity)
  * )
  * and su_nationkey = n_nationkey and n_name = 'Germany'
  * order by su_name
  */
class Q20 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orderline = dfReader.option("table", "order-line").load()
    val stock = dfReader.option("table", "stock").load()
    val supplier = dfReader.option("table", "supplier").load()
    val nation = dfReader.option("table", "nation").option("useSmallMemory", "true").load()
    val item = dfReader.option("table", "item").load()

    //select i_id from item where i_data like 'co%'
    val fitem = item.filter($"i_data".like("co%")).select($"i_id")
    val forderline = orderline.filter($"ol_delivery_d" > 20100523)
    val fnation = nation.filter($"n_name" === "Germany")

    val inner_query = stock
      .join(forderline, (forderline("ol_i_id") === $"s_i_id"))
      .join(fitem, $"s_i_id".isin(fitem("i_id"))) //TODO do with a join with item?
      .groupBy($"s_i_id", $"s_w_id", $"s_quantity")
      .agg(sum($"ol_quantity").as("sum_qty"))
      .filter($"s_quantity".*(2) > $"sum_qty")
      .select((($"s_i_id" * $"s_w_id") % 10000).as("inner_suppkey"))

    //TODO verify the <isin> operation
    supplier.join(fnation, fnation("n_nationkey") === $"su_nationkey")
      .join(inner_query, $"su_suppkey" === inner_query("inner_suppkey"))
      .filter($"su_suppkey".isin(inner_query("inner_suppkey")))

  }
}
