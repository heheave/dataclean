package avgcache


/**
  * Created by xiaoke on 17-6-2.
  */
sealed case class Avg(at: String, interval: Long) {

  def avgName(): String = at

  def format(timestamp: Long): Long = timestamp / interval * interval

  def getInterval(): Long = interval

  override def equals(obj: scala.Any): Boolean = obj match {
    case avg: Avg => {
      interval == avg.getInterval()
    }
    case _ => false
  }

  override def hashCode(): Int = (interval ^ (interval >>> 32)).toInt
}

object AvgFactory {

  val dayAvg = Avg("DAY", 24* 60 * 60 * 1000)

  val hourAvg = Avg("HOUR", 60 * 60 * 1000)

  val minAvg = Avg("MIN", 60 * 1000)

  def otherAvg(interval: Long) = Avg("INTERVAL%s".format(interval), interval)
}



