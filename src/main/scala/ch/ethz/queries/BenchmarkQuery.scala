package ch.ethz.queries

import ch.ethz.StorageEngine
import org.apache.spark.sql.{DataFrameReader, SQLContext, DataFrame}
import java.util.Calendar

import scala.collection.mutable

class BenchmarkQuery {

  // have the storage type to know how to query
  var storageType:StorageEngine.Value = null
  val inputPath:String = ""

  // have the reference date as it appears in many places
  val calendar = Calendar.getInstance()

  val referenceDate1994: Long = {
    calendar.set(1994, 1, 1)
    calendar.getTimeInMillis
  }

  val referenceDate19960401: Long = {
    calendar.set(1996, 4, 1)
    calendar.getTimeInMillis
  }

  val referenceDate19960101: Long = {
    calendar.set(1996, 1, 1)
    calendar.getTimeInMillis
  }

  val referenceDate19951001: Long = {
    calendar.set(1995, 10, 1)
    calendar.getTimeInMillis
  }

  val referenceDate19950901: Long = {
    calendar.set(1995, 9, 1)
    calendar.getTimeInMillis
  }

  val referenceDate19961231: Long = {
    calendar.set(1996, 12, 31)
    calendar.getTimeInMillis
  }

  val referenceDate1995: Long = {
    calendar.set(1995, 1, 1)
    calendar.getTimeInMillis
  }

  val referenceDate19980902: Long = {
    calendar.set(1998, 9, 2)
    calendar.getTimeInMillis
  }

  val referenceDate19930315: Long = {
    calendar.set(1993, 3, 15)
    calendar.getTimeInMillis
  }

  val referenceDate19930701: Long = {
    calendar.set(1993, 7, 1)
    calendar.getTimeInMillis
  }

  val referenceDate1993: Long = {
    calendar.set(1993, 10, 1)
    calendar.getTimeInMillis
  }

  val referenceDate1999: Long = {
    calendar.set(1999, 1, 1)
    calendar.getTimeInMillis
  }

  val referenceDate2007: Long = {
    calendar.set(2007, 1, 2)
    calendar.getTimeInMillis
  }

  val referenceDate2010: Long = {
    calendar.set(2010, 5, 23, 12, 0)
    calendar.getTimeInMillis
  }

  val referenceDate2012: Long = {
    calendar.set(2012, 1, 2)
    calendar.getTimeInMillis
  }

  val referenceDate2020First: Long = {
    calendar.set(2020, 1, 1)
    calendar.getTimeInMillis
  }

  val referenceDate2020Second: Long = {
    calendar.set(2020, 1, 2)
    calendar.getTimeInMillis
  }

  /**
    * implemented in children classes and hold the actual query
    */
  def executeQuery(sqlCxt: SQLContext, dfReader: DataFrameReader): DataFrame = ???
  def executeQuery(p: (SQLContext, DataFrameReader)): DataFrame = executeQuery(p._1, p._2)

  def getTableOptions(tabName: String, option: (String, String)*): mutable.Map[String, String] = {
    var options = mutable.Map[String, String]()
    storageType match {
      case StorageEngine.TELL => {
        options += ("table" -> tabName)
      }
      case StorageEngine.KUDU => {
        options += ("kudu.table" -> tabName)
      }
      case StorageEngine.PARQUET => {
        options += ("path" -> (inputPath + tabName + ".parquet"))
      }
      case _ => throw new IllegalArgumentException(s"Storage type not supported: ${storageType.toString}")
    }
    option.map(options += _)
    options
  }
}
