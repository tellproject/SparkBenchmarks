package ch.ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query22
  *
  * select
  *  substr(c_state,1,1) as country, count(*) as numcust, sum(c_balance) as totacctbal
  * from	 customer
  * where substr(c_phone,1,1) in ('1','2','3','4','5','6','7')
  * and c_balance > (
  *     select avg(c_BALANCE)
  *    from customer
  *    where  c_balance > 0.00
  *    and substr(c_phone,1,1) in ('1','2','3','4','5','6','7')
  * )
  * and not exists (
  *    select *
  *    from	orders
  *    where	o_c_id = c_id and o_w_id = c_w_id and o_d_id = c_d_id
  * )
  * group by substr(c_state,1,1)
  * order by substr(c_state,1,1)
  */
class Q22 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orders = dfReader.option("table", "order").load()
    val customer = dfReader.option("table", "customer").load()

    val fcustomer = customer.filter($"c_phone".substr(1, 1).isin("1", "2", "3", "4", "5", "6", "7"))
    val avg_cbal = fcustomer.filter($"c_balance" > 0).select($"c_balance").agg(avg($"c_balance").as("avg_balance"))

    fcustomer.join(orders,
      ($"c_id" !== orders("o_c_id")) && ($"c_w_id" !== orders("o_w_id")) && ($"c_d_id" !== orders("o_d_id"))
    )
      .join(avg_cbal, $"c_balance" > avg_cbal("avg_balance")) //.filter($"c_balance" > avg_cbal("avg_balance"))
      .select($"c_state".substr(1, 1).as("country"), $"c_balance")
      .groupBy($"country")
      .agg(count("c_balance").as("numcust"), sum("c_balance").as("totacctbal"))

  }
}
