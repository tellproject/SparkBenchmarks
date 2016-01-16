package ch.ethz

import ch.ethz.queries.BenchmarkQuery
import ch.ethz.tell.TellContext
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.{SparkContext, SparkConf, Logging}

object StorageEngine extends Enumeration {
  val KUDU = Value("kudu")
  val TELL = Value("tell")
  val PARQUET = Value("parquet")
}

object QueryRunner extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Specify storage engine, then benchmark")
      println("\t <kudu|tell|parquet> <chb|tpch> <limitQuery> <inputPath>")
      return
    }
    var nQueries = 22
    val strEngine = StorageEngine.withName(args(0))
    val benchmark = args(1)
    var parquetInputPath = ""
    if (args.length == 3)
      nQueries = args(2).toInt
    if (strEngine.equals(StorageEngine.PARQUET)) {
      if (args.length == 4)
        parquetInputPath = args(3)
      else
        println("Input path required for Parquet file")
    }

    val conf = new SparkConf()
    //conf.set("spark.sql.tell.chunkSizeBig", (2L * 1024L * 1024L * 1024L).toString)

    val sc = new SparkContext(conf)

    logWarning("Starting warm up query.")
    val query = Class.forName(f"ch.ethz.queries.${benchmark}.Q1").newInstance.asInstanceOf[BenchmarkQuery]
    query.storageType = strEngine
    query.inputPath = parquetInputPath
    val sqlApiEntry = initializeExec(sc, strEngine)
    query.executeQuery(sqlApiEntry).count
    logWarning("Finished warm up query.")

    val queryOrder = List(6, 14, 19, 16, 4, 12, 13, 1, 10, 3, 15, 2, 18, 20, 17, 7, 5, 8, 9, 22, 21, 11)
    queryOrder.foreach(i => {
      val query = Class.forName(f"ch.ethz.queries.${benchmark}.Q${i}%d").newInstance.asInstanceOf[BenchmarkQuery]
      query.storageType = strEngine
      query.inputPath = parquetInputPath

      logInfo(s"Running query ${i}")
      val start = System.nanoTime()

      val sqlApiEntry = initializeExec(sc, strEngine)
      val data = query.executeQuery(sqlApiEntry)
      // logWarning("Started dhowing data")
      // data.limit(10).collect().map(r => logWarning(s"[P] ${r.toString()}"))
      // logWarning("Finished showing data")
      // data.show(10)
      val cnt = data.collect().length
      finalizeExec(sqlApiEntry, strEngine)

      val end = System.nanoTime()
      logWarning(s"Running query ${i} took ${(end - start) / 1000000}ms and produced ${cnt} tuples.")
    })
  }

  def initializeExec(sc: SparkContext, st: StorageEngine.Value): (SQLContext, DataFrameReader) = {
    var sqlCxt: SQLContext = null
    var dfReader: DataFrameReader = null
    st match {
      case StorageEngine.TELL => {
        val tellCxt = new TellContext(sc)
        dfReader = tellCxt.read.format("ch.ethz.tell")
        var numParts = sc.getConf.get("spark.sql.tell.numPartitions")
        dfReader.option("numPartitions", numParts)
        tellCxt.startTransaction()
        sqlCxt = tellCxt
      }
      case StorageEngine.KUDU => {
        sqlCxt = new SQLContext(sc)
        val kuduMaster = sc.getConf.get("spark.sql.kudu.master")
        dfReader = sqlCxt.read.format("org.kududb.spark").option("kudu.master", kuduMaster)
      }
      case StorageEngine.PARQUET => {
        sqlCxt = new SQLContext(sc)
        dfReader = sqlCxt.read.format("parquet")
      }
      case _ => throw new IllegalArgumentException(s"Storage engine not supported: ${st}")
    }
    // repartitioning for 8 number of cores

    (sqlCxt, dfReader)
  }

  def finalizeExec = new {
    def apply(sqlApiEntry: (SQLContext, DataFrameReader), st: StorageEngine.Value) = (sqlApiEntry._1, st)

    def apply(sqlCxt: SQLContext, st: StorageEngine.Value) = {
      st match {
        case StorageEngine.TELL => {
          sqlCxt.asInstanceOf[TellContext].commitTransaction()
        }
      }
    }
  }
}
