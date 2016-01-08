package ch.ethz

import ch.ethz.queries.BenchmarkQuery
import ch.ethz.tell.TellContext
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.{SparkContext, SparkConf, Logging}

object StorageEngine extends Enumeration {
  val KUDU = Value("kudu")
  val TELL = Value("tell")
  val HDFS = Value("hdfs")
}

object QueryRunner extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Specify storage engine, then benchmark")
      println("\t <kudu|tell|hdfs> <chb|tpch>")
      return
    }
    val strEngine = StorageEngine.withName(args(0))
    val benchmark = args(1)

    val conf = new SparkConf()
    conf.set("spark.sql.tell.chunkSizeBig", (2L * 1024L * 1024L * 1024L).toString)

    val sc = new SparkContext(conf)

    val i: Int = 1
    val query = Class.forName(f"ch.ethz.queries.${benchmark}.Q${i}%d").newInstance.asInstanceOf[BenchmarkQuery]
    query.storageType = strEngine

    logInfo(s"Running query ${i}")
    val start = System.nanoTime()

    val sqlApiEntry = initializeExec(sc, strEngine)
    val data = query.executeQuery(sqlApiEntry)
    data.show(100)
    finalizeExec(sqlApiEntry, strEngine)

    val end = System.nanoTime()
    logInfo(s"Running query ${i} took ${(end - start) / 1000000}ms")
  }

  def initializeExec(sc: SparkContext, st: StorageEngine.Value): (SQLContext, DataFrameReader) = {
    var sqlCxt: SQLContext = null
    var dfReader: DataFrameReader = null
    st match {
      case StorageEngine.TELL => {
        val tellCxt = new TellContext(sc)
        dfReader = tellCxt.read.format("tell").option("numPartitions", "8")
        tellCxt.startTransaction()
        sqlCxt = tellCxt
      }
      case StorageEngine.KUDU => {
        sqlCxt = new SQLContext(sc)
        val kuduMaster = sc.getConf.get("spark.sql.kudu.master")
        dfReader = sqlCxt.read.format("org.kududb.spark").option("kudu.master", kuduMaster)
      }
      case StorageEngine.HDFS => {
        //TODO read parquet files
        sqlCxt = new SQLContext(sc)
        throw new IllegalArgumentException(s"Storage engine not supported: ${st}")
      }
      case _ => throw new IllegalArgumentException(s"Storage engine not supported: ${st}")
    }
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
