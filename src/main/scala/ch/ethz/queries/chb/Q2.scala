package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query2
  *
  * select su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
  * from item, supplier, stock, nation, region, (Inner_Query) as m
  * where i_id = s_i_id and mod((s_w_id * s_i_id), 10000) = su_suppkey and su_nationkey = n_nationkey
  * and n_regionkey = r_regionkey and i_data like '%b' and r_name like 'Europ%'
  * and i_id=m_i_id and s_quantity = m_s_quantity
  * order by n_name, su_name, i_id
  */
class Q2 extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val region = dfReader.options(getTableOptions("region", ("useSmallMemory" -> "true"))).load()
    val nation = dfReader.options(getTableOptions("nation", ("useSmallMemory" -> "true"))).load()
    val supplier = dfReader.options(getTableOptions("supplier")).load()
    val stock = dfReader.options(getTableOptions("stock")).load()
    val item = dfReader.options(getTableOptions("item")).load()


    /**
      * Inner_Query
      * (select s_i_id as m_i_id, min(s_quantity) as m_s_quantity from
      * stock, supplier, nation, region
      * where mod((s_w_id*s_i_id),10000)=su_suppkey and su_nationkey=n_nationkey
      * and n_regionkey=r_regionkey and r_name like 'Europ%' group by s_i_id) m
      */
    val minEuQty = stock.join(supplier, (stock("s_w_id") * stock("s_i_id") % 10000) === supplier("su_suppkey"))
      .join(nation, $"su_nationkey" === nation("n_nationkey"))
      .join(region, $"n_regionkey" === region("r_regionkey"))
      .filter(region("r_name").startsWith("Europ"))
      .groupBy($"s_i_id")
      .agg(min($"s_quantity").as("m_s_quantity")).select($"s_i_id".as("m_i_id"), $"m_s_quantity")

    stock
      .join(item, $"s_i_id" === item("i_id"))
      .join(supplier, (stock("s_w_id") * stock("s_i_id") % 10000) === supplier("su_suppkey"))
      .join(nation, $"su_nationkey" === nation("n_nationkey"))
      .join(region, $"n_regionkey" === region("r_regionkey"))
      .filter(item("i_data").endsWith("b"))
      .filter(region("r_name").startsWith("Europ"))
      .join(minEuQty, (($"i_id" === minEuQty("m_i_id")) && ($"s_quantity" === minEuQty("m_s_quantity"))))
      .orderBy(nation("n_name"), supplier("su_name"), item("i_id"))
  }
}
