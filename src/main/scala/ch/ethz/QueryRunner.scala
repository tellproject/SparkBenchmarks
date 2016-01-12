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

    (1 to nQueries).map(i => {
      val query = Class.forName(f"ch.ethz.queries.${benchmark}.Q${i}%d").newInstance.asInstanceOf[BenchmarkQuery]
      query.storageType = strEngine
      query.inputPath = parquetInputPath

      logInfo(s"Running query ${i}")
      val start = System.nanoTime()

      val sqlApiEntry = initializeExec(sc, strEngine)
      val data = query.executeQuery(sqlApiEntry)
   //   data.show(100)
      data.count()
      finalizeExec(sqlApiEntry, strEngine)

      val end = System.nanoTime()
      logWarning(s"Running query ${i} took ${(end - start) / 1000000}ms")
    })
  }

  def initializeExec(sc: SparkContext, st: StorageEngine.Value): (SQLContext, DataFrameReader) = {
    var sqlCxt: SQLContext = null
    var dfReader: DataFrameReader = null
    st match {
      case StorageEngine.TELL => {
        val tellCxt = new TellContext(sc)
        dfReader = tellCxt.read.format("ch.ethz.tell")
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
    dfReader.option("numPartitions", "8")
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
