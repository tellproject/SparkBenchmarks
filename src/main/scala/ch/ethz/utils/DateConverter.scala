package ch.ethz.utils

import org.joda.time.format.DateTimeFormat

object DateConverter {

  def dateToLong(dateString: String): Long = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    formatter.parseLocalDate(dateString).toDate.getTime
  }
}
