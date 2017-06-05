package action

/**
  * Created by xiaoke on 17-6-1.
  */
object Actions {

  val AVG_NONE = 0
  val AVG_HOUR = 1
  val AVG_MIN = 1 << 1
  val AVG_DAY = 1 << 2

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
