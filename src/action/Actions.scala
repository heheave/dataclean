package action

import avgcache.{Avg, AvgFactory}

/**
  * Created by xiaoke on 17-6-1.
  */
object Actions {

  val XMARK = 'X'

  val AVG_HOUR = Array(AvgFactory.hourAvg)
  val AVG_MIN = Array(AvgFactory.minAvg)
  val AVG_DAY = Array(AvgFactory.dayAvg)

  def getActions(avgStr: String): Option[Array[Avg]] = {
    if(avgStr == null) {
      None
    } else {
      val avgs = avgStr.split(":")
      val result = new Array[Avg](avgs.length)
      var idx = 0
      while (idx < avgs.length) {
        val avg = avgs(idx) match {
          case "MIN" => AvgFactory.minAvg
          case "HOUR" => AvgFactory.hourAvg
          case "DAY" => AvgFactory.dayAvg
          case _ => {
            try {
              AvgFactory.otherAvg(avgs(idx).toLong)
            } catch {
              case e: Throwable => null
            }
          }
        }
        result(idx) = avg
        idx += 1
      }
      result.filter(_!=null)
      if (result.length < 1) {
        None
      } else {
        Some(result)
      }
    }
  }

  def objToDouble(obj: Any): Double = obj match {
    case b: Boolean => if (b) 1 else 0
    case s: Short => s
    case i: Int => i
    case d: Double => d
    case f: Float => f.toDouble
    case l: Long => l.toDouble
        case s: String => s.toDouble
    case _ => throw new IllegalArgumentException("Class[%s] cannot be cast to double".format(obj.getClass.getName))
  }

}
