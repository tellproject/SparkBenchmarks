package ch.ch.ethz.queries.chb

import java.time.Instant

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.sql.functions.udf

/**
  * Ch Query8
  *
  * select	 extract(year from o_entry_d) as l_year,
  * sum(case when n2.n_name = 'Germany' then ol_amount else 0 end) / sum(ol_amount) as mkt_share
  * from	 item, supplier, stock, orderline, orders, customer, nation n1, nation n2, region
  * where	 i_id = s_i_id and i_id = ol_i_id and ol_i_id = s_i_id and
  * ol_supply_w_id = s_w_id
  * and mod((s_w_id * s_i_id),10000) = su_suppkey
  * and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
  * and c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
  * and n1.n_nationkey = ascii(substr(c_state,1,1))
  * and n1.n_regionkey = r_regionkey
  * and ol_i_id < 1000
  * and r_name = 'Europe'
  * and su_nationkey = n2.n_nationkey
  * and o_entry_d between '2007-01-02 00:00:00.000000' and '2012-01-02 00:00:00.000000'
  * and i_data like '%b'
  * group by extract(year from o_entry_d)
  * order by l_year
  */
class Q8 extends BenchmarkQuery {

  val getYear = udf { (x: Long) => Instant.ofEpochSecond(x).toString.substring(0, 4) }
  val mkr_share = udf { (x: String, y: Double) => if (x.equals("Germany")) y else 0 }

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val orders = dfReader.option("table", "order").load()
    val customer = dfReader.option("table", "customer").load()
    val orderline = dfReader.option("table", "order-line").load()
    val stock = dfReader.option("table", "stock").load()
    val supplier = dfReader.option("table", "supplier").load()
    val nation = dfReader.option("table", "nation").option("useSmallMemory", "true").load()
    val region = dfReader.option("table", "region").option("useSmallMemory", "true").load()
    val item = dfReader.option("table", "item").load()

    // supplier, stock, orderline, orders, customer, nation n1, nation n2
    val forderline = orderline.filter($"ol_i_id" < 1000)

    val n1 = nation
    val n2 = nation
    val forder = orders.filter($"o_entry_d".between(20070102, 20120102))
    val fregion = region.filter($"r_name" === "Europe")

    val fitem = item.filter($"i_data".like("%b"))
    val s_n2 = supplier.join(n2, $"su_nationkey" === n2("n_nationkey"))
    val r_n1 = fregion.join(n1, $"r_regionkey" === n1("n_regionkey"))

    //mod((s_w_id * s_i_id),10000) = su_suppkey
    val part_res1 = stock.join(s_n2, ($"s_w_id" * $"s_i_id") % 10000 === s_n2("su_suppkey"))
      //and ol_i_id = s_i_id and ol_supply_w_id = s_w_id
      .join(forderline, ($"s_i_id" === forderline("ol_i_id") && ($"s_w_id" === forderline("ol_supply_w_id"))))
      .join(fitem, (($"i_id" === $"s_i_id") && ($"i_id" === $"ol_i_id")))

    // n1.n_nationkey = ascii(substr(c_state,1,1)) and
    // c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
    val part_res2 = customer.join(r_n1, $"c_state".substr(1, 1) === r_n1("n_nationkey"))
      .join(forder, (($"o_c_id" === $"c_id") && ($"c_w_id" === $"o_w_id") && ($"c_d_id" === $"o_d_id")))

    //ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
    part_res1.join(part_res2,
      (($"ol_w_id" === $"o_w_id") &&
        ($"ol_d_id" === $"o_d_id") &&
        ($"ol_o_id" === $"o_id")))
      // todo check the "first function"
      .select(getYear($"o_entry_d").as("l_year"), $"ol_amount", part_res2("n_name"))
      .groupBy($"l_year")
      .agg(sum(mkr_share(part_res2("n_name"), $"ol_amount")) / sum($"ol_amount"))

  }
}
