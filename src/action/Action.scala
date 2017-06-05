package action

/**
  * Created by xiaoke on 17-5-31.
  */
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox


trait Action extends Serializable{
  def avgType(): Int
  def transferedV(originV: Double): Double
}

case class AssAction(ass: Double, avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = ass
}

case class AddAction(addV: Double, avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = addV + originV
}

case class NegAction(avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = -originV
}

case class SubAction(subV: Double, avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = subV - originV
}

case class MulAction(mulV: Double, avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = originV * mulV
}

case class DivAction(divV: Double, avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double =
    if (originV != 0) {
      divV / originV
    } else {
      throw new IllegalArgumentException("Div num should be zero: %f/%f".format(divV, originV))
    }
}

case class RvsAction(avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = if (originV != 0) {
    1 / originV
  } else {
    throw new IllegalArgumentException("Div num should be zero: 1/%f".format(originV))
  }
}

case class ExprAction(expr: Array[Action], avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = {
    var res = originV
    expr.foreach(action => res = action.transferedV(res))
    res
  }
}

case class FunAction(fun: (Double) => Double, avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = fun(originV)
}

case class StrAction(str: String, avg: Int = Actions.AVG_NONE) extends Action {
  override def avgType() = avg
  override def transferedV(originV: Double): Double = {
    val toolBox = currentMirror.mkToolBox()
    val tree = toolBox.parse(str.replaceAll("\\$X", originV.toString))
    val res = toolBox.eval(tree).asInstanceOf[Double]
    res
  }
}
