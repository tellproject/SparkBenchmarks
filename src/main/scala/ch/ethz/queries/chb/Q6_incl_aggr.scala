//package ch.ch.ethz.queries
//
//import ch.ch.ethz.queries.chb.ChTSchema
//import ch.ch.ethz.tell.Field.FieldType
//import ch.ch.ethz.tell.ScanQuery.AggrType
//import ch.ch.ethz.tell._
//
///**
// * Query6
// * select	sum(ol_amount) as revenue
// * from	orderline
// * where	ol_delivery_d >= '1999-01-01 00:00:00.000000'
// * and ol_delivery_d < '2020-01-01 00:00:00.000000'
// * and ol_quantity between 1 and 100000
// */
////class Q6Aggr extends ChQuery {
////
////  override def execute(st: String, cm: String, cn:Int, cs:Long, mUrl:String): Unit = {
////    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
////    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
////    import org.apache.spark.sql.functions._
////    import sqlContext.implicits._
////
////    // prepare date selection
////    val oSchema = ChTSchema.orderLineSch
////    val orderLineQuery = new ScanQuery
////    val oDeliveryIndex = oSchema.getField("ol_delivery_d").index
////
////    val dateSelectionLower = new CNFClause
////    dateSelectionLower.addPredicate(
////    ScanQuery.CmpType.GREATER_EQUAL, oDeliveryIndex, referenceDate1999)
////    orderLineQuery.addSelection(dateSelectionLower)
////
////    val dateSelectionUpper = new CNFClause
////    dateSelectionUpper.addPredicate(
////    ScanQuery.CmpType.LESS, oDeliveryIndex, referenceDate2020First)
////    // as anything satisfy the query, we can leave this filter away
//////    orderLineQuery.addSelection(dateSelectionUpper)
////
////    // prepare quantity selection
////    val oQuantityIndex = oSchema.getField("ol_quantity").index
////    val quantitySelectionLower = new CNFClause
////    quantitySelectionLower.addPredicate(
////      ScanQuery.CmpType.GREATER_EQUAL, oQuantityIndex, PredicateType.create(1: Short))
////    orderLineQuery.addSelection(quantitySelectionLower)
////
////    val quantitySelectionUpper = new CNFClause
////    quantitySelectionUpper.addPredicate(
////      ScanQuery.CmpType.LESS, oQuantityIndex, PredicateType.create(100: Short))
////    // the original benchmark says 100000 which is not a numeric(2)!!
////    // as anything satisfy the query, we can leave this filter away
////    //orderLineQuery.addSelection(quantitySelectionUpper)
////
////    val olAmountIndex = oSchema.getField("ol_amount").index
////    val amountAggregation = new Aggregation(AggrType.SUM, olAmountIndex, "sum_ol_amount", FieldType.BIGINT)
////    orderLineQuery.addAggregation(amountAggregation)
////
////    val resultSchema = orderLineQuery.getResultSchema
////
////    // convert an RDDs to a DataFrames
////    val aggrTrdd = new TRDD[TRecord](scc, "order-line", orderLineQuery, new TSchema(resultSchema)).map(r => {
////      r.getValue("sum_ol_amount").asInstanceOf[Long]
////    })
////
////    val t0 = System.nanoTime()
////
////    var amount:Long = 0
////    val res = aggrTrdd.collect().map(r => amount += r)
////
////    val t1 = System.nanoTime()
////    logger.warn("[Query %d] Elapsed time: %d msecs. map:%d".format(6, (t1 - t0) / 1000000, amount))
////    scc.sparkContext.stop()
////  }
////}
