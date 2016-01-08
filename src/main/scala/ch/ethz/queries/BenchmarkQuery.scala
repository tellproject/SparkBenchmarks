package ch.ethz.queries

import ch.ethz.StorageEngine
import org.apache.spark.sql.{DataFrameReader, SQLContext, DataFrame}
import java.util.Calendar

import scala.collection.mutable

class BenchmarkQuery {

  // have the storage type to know how to query
  var storageType:StorageEngine.Value = null

  // have the reference date as it appears in many places
  val calendar = Calendar.getInstance()

  val referenceDate1999: Long = {
    calendar.set(1999, 1, 1)
    calendar.getTimeInMillis * 1000L * 1000L // create nano seconds
  }

  val referenceDate2007: Long = {
    calendar.set(2007, 1, 2)
    calendar.getTimeInMillis * 1000L * 1000L // create nano seconds
  }

  val referenceDate2010: Long = {
    calendar.set(2010, 5, 23, 12, 0)
    calendar.getTimeInMillis * 1000L * 1000L // create nano seconds
  }

  val referenceDate2012: Long = {
    calendar.set(2012, 1, 2)
    calendar.getTimeInMillis * 1000L * 1000L // create nano seconds
  }

  val referenceDate2020First: Long = {
    calendar.set(2020, 1, 1)
    calendar.getTimeInMillis * 1000L * 1000L // create nano seconds
  }

  val referenceDate2020Second: Long = {
    calendar.set(2020, 1, 2)
    calendar.getTimeInMillis * 1000L * 1000L // create nano seconds
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
      case _ => throw new IllegalArgumentException(s"Storage type not supported: ${storageType.toString}")
    }
    option.map(options += _)
    options
  }
}
