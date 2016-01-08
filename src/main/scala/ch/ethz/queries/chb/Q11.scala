package ch.ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query11
  *
  * select	 s_i_id, sum(s_order_cnt) as ordercount
  * from	 stock, supplier, nation
  * where	 mod((s_w_id * s_i_id),10000) = su_suppkey
  * and su_nationkey = n_nationkey
  * and n_name = 'Germany'
  * group by s_i_id
  * having   sum(s_order_cnt) >
  * (  select sum(s_order_cnt) * .005
  *   from stock, supplier, nation
  *   where mod((s_w_id * s_i_id),10000) = su_suppkey
  *   and su_nationkey = n_nationkey
  *   and n_name = 'Germany'
  * )
  * order by ordercount desc
  */
class Q11 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val nation = dfReader.options(getTableOptions("nation", ("useSmallMemory" -> "true"))).load()
    val supplier = dfReader.options(getTableOptions("supplier")).load()
    val stock = dfReader.options(getTableOptions("stock")).load()

    val fnation = nation.filter($"n_name" === "Germany")
    val inner_res = supplier.join(fnation, $"su_nationkey" === fnation("n_nationkey"))
      .join(stock, $"su_suppkey" === (stock("s_w_id") * stock("s_i_id") % 10000))
      .select($"s_order_cnt")
      .agg((sum($"s_order_cnt") * 0.005).as("sum_order"))

    supplier.join(fnation, $"su_nationkey" === fnation("n_nationkey"))
      .join(stock, $"su_suppkey" === (stock("s_w_id") * stock("s_i_id") % 10000))
      .select($"s_i_id", $"s_order_cnt")
      .groupBy($"s_i_id")
      .agg(sum($"s_order_cnt").as("ordercount"))
      //TODO to be checked
      .join(inner_res, $"ordercount" > inner_res("sum_order"))

  }
}
