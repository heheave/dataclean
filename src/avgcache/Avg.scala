package avgcache

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by xiaoke on 17-6-2.
  */
trait Avg {

  def avgType(): String

  def format(timestamp: Long): String

  def getInterval(): Long
}

sealed class DayAvg() extends Avg {

  override def avgType = DayAvg.avgType

  override def format(timestamp: Long): String = {
    DayAvg.format.format(new Date(timestamp))
  }

  override def getInterval(): Long = DayAvg.interval
}

object DayAvg {

  private val avgType = "DAY"

  private val interval = 24* 60 * 60 * 1000

  private val format = new SimpleDateFormat("yyyy_MM_dd")

  def apply() = new HourAvg()
}

sealed class HourAvg() extends Avg {

  override def avgType = HourAvg.avgType

  override def format(timestamp: Long): String = {
      HourAvg.format.format(new Date(timestamp))
  }

  override def getInterval(): Long = HourAvg.interval
}

object HourAvg {

  private val avgType = "HOUR"

  private val interval = 60 * 60 * 1000

  private val format = new SimpleDateFormat("yyyy_MM_dd_HH")

  def apply() = new HourAvg()
}

sealed class MinAvg() extends Avg {

  override def avgType = MinAvg.avgType

  override def format(timestamp: Long): String = {
    MinAvg.format.format(new Date(timestamp))
  }

  override def getInterval(): Long = MinAvg.interval
}

object MinAvg {

  private val avgType = "MIN"

  private val interval = 60 * 1000

  private val format = new SimpleDateFormat("yyyy_MM_dd_HH_mm")

  def apply() = new MinAvg()
}


