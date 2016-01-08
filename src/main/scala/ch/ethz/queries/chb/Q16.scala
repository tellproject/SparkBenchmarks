package ch.ethz.queries.chb

import ch.ethz.queries.BenchmarkQuery
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Ch Query16
  *
  * select	 i_name,
  *   substr(i_data, 1, 3) as brand,
  *   i_price,
  *   count(distinct (mod((s_w_id * s_i_id),10000))) as supplier_cnt
  * from	 stock, item
  * where	 i_id = s_i_id
  * and i_data not like 'zz%' and (mod((s_w_id * s_i_id),10000) not in
  *   (select su_suppkey from supplier where su_comment like '%bad%'))
  * group by i_name, substr(i_data, 1, 3), i_price
  * order by supplier_cnt desc
 */
class Q16  extends BenchmarkQuery {

  override def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = {

    import org.apache.spark.sql.functions._
    import sqlCxt.implicits._

    val stock = dfReader.option("table", "stock").load()
    val supplier = dfReader.option("table", "supplier").load()
    val item = dfReader.option("table", "item").load()

    val fsupplier = supplier
      .filter($"su_comment".like("%bad%"))
      .select($"su_suppkey")

    val fitem = item
    .filter(!$"i_data".like("zz%"))

   stock.join(fsupplier, ( ($"s_w_id" * $"s_i_id")%10000 !== (fsupplier("su_suppkey")) ))
    .join(fitem, $"i_id" === $"s_i_id")
    .select($"i_name", $"i_data".substr(1, 3).as("brand"), $"i_price", $"s_w_id", $"s_i_id")
    .groupBy($"i_name",$"brand", $"i_price")
    .agg(countDistinct(($"s_w_id" * $"s_i_id")%10000).as("supplier_cnt"))
    .orderBy("supplier_cnt")

  }
}
