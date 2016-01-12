package ch.ethz.utils

import java.util.Calendar

object DateConverter {

  def dateToLong(dateString: String): Long = {
    val calendar = Calendar.getInstance()
    val dd = dateString.split('-')
    calendar.set(dd(0).toInt, dd(1).toInt, dd(2).toInt)
    calendar.getTimeInMillis
  }

}
