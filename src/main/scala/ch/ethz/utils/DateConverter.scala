package ch.ethz.utils

import java.util.Calendar

object DateConverter {

  def dateToLong(dateString: String): Long = {
    val calendar = Calendar.getInstance()
    dateString.split('-').map(p => calendar.set(p(0).toInt, p(1).toInt, p(2).toInt))
    calendar.getTimeInMillis
  }

}
