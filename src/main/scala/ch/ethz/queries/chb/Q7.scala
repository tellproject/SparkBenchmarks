package ch.ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

import java.time.Instant

import org.apache.spark.sql.functions.udf

/**
  * Ch Query7
  * select
  *   su_nationkey as supp_nation,
  *   substr(c_state,1,1) as cust_nation,
  *   extract(year from o_entry_d) as l_year,
  *   sum(ol_amount) as revenue
  * from supplier, stock, orderline, orders, customer, nation n1, nation n2
  * where
  *   ol_supply_w_id = s_w_id and ol_i_id = s_i_id and mod((s_w_id * s_i_id), 10000) = su_suppkey
  *   and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id and c_id = o_c_id and c_w_id = o_w_id
  *   and c_d_id = o_d_id and su_nationkey = n1.n_nationkey and ascii(substr(c_state,1,1)) = n2.n_nationkey
  *   and ( (n1.n_name = 'Germany' and n2.n_name = 'Cambodia') or (n1.n_name = 'Cambodia' and n2.n_name = 'Germany') )
  *   and ol_delivery_d between '2007-01-02 00:00:00.000000' and '2012-01-02 00:00:00.000000'
  * group by su_nationkey, substr(c_state,1,1), extract(year from o_entry_d)
  * order by su_nationkey, cust_nation, l_year
  */
class Q7 extends BenchmarkQuery {

  val getYear = udf { (x: Long) => Instant.ofEpochSecond(x).toString.substring(0, 4) }

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val customer = dfReader.options(getTableOptions("customer")).load()
    val orders = dfReader.options(getTableOptions("order")).load()
    val orderline = dfReader.options(getTableOptions("order-line")).load()
    val nation = dfReader.options(getTableOptions("nation", ("useSmallMemory" -> "true"))).load()
    val supplier = dfReader.options(getTableOptions("supplier")).load()
    val stock = dfReader.options(getTableOptions("stock")).load()

    // we know that the filter on dates below 2012, returns 0 results
    // val forderline = orderline.filter($"ol_o_id" <= 0)
    val forderline = orderline.filter($"ol_delivery_d" >= 20070102 && $"ol_delivery_d" <= 20120102)
    val n1 = nation
    val n2 = nation

    val suppNation = supplier.join(n1, $"su_nationkey" === n1("n_nationkey"))
      .join(n2,
        (((n1("n_name") === "Germany") && (n2("n_name") === "Cambodia")) ||
          ((n1("n_name") === "Cambodia") && (n2("n_name") === "Germany"))))

    val part_res = customer.join(suppNation, n2("n_nationkey") === customer("c_state").substr(1, 1))
      .join(orders, (orders("o_c_id") === customer("c_id")) &&
        (orders("o_w_id") === customer("c_w_id")) &&
        (orders("o_d_id") === customer("c_d_id")))
      .join(forderline, ((forderline("ol_w_id") === orders("o_w_id")) &&
        (forderline("ol_d_id") === orders("o_d_id")) &&
        (forderline("ol_o_id") === orders("o_id"))))
      .join(stock, ((stock("s_w_id") === forderline("ol_supply_w_id")) &&
        (stock("s_i_id") === forderline("ol_i_id")) &&
        (stock("s_i_id") * stock("s_w_id") % 10000 === suppNation("su_suppkey"))))

    part_res
      .select($"su_nationkey".as("supp_nation"),
        $"c_state".substr(1, 1).as("cust_nation"),
        getYear($"o_entry_d").as("l_year"),
        $"ol_amount"
      )
      .groupBy($"supp_nation", $"cust_nation", $"l_year") //customer("c_state").substr(1,1), getYear(order("o_entry_d")))
      .agg(sum("ol_amount").as("revenue"))
      .sort($"supp_nation", $"cust_nation", $"l_year")
  }
}

