package action

/**
  * Created by xiaoke on 17-5-31.
  */

import action.expression.ExprUtil
import avgcache.Avg

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox


trait Action extends Serializable{

  def avgType(): Array[Avg]

  def transferedV(originV: Double): Double
}

case class AssAction(ass: Double, avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = ass
}

case class AddAction(addV: Double, avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = addV + originV
}

case class NegAction(avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = -originV
}

case class SubAction(subV: Double, avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = subV - originV
}

case class MulAction(mulV: Double, avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = originV * mulV
}

case class DivAction(divV: Double, avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double =
    if (originV != 0) {
      divV / originV
    } else {
      throw new IllegalArgumentException("Div num should be zero: %f/%f".format(divV, originV))
    }
}

case class RvsAction(avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = if (originV != 0) {
    1 / originV
  } else {
    throw new IllegalArgumentException("Div num should be zero: 1/%f".format(originV))
  }
}

case class ExprAction(expr: Array[Action], avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = {
    var res = originV
    expr.foreach(action => res = action.transferedV(res))
    res
  }
}

case class Expr1Action(str: String, avg: Option[Array[Avg]] = None) extends Action {
  private val expr = ExprUtil.fromString(str, Actions.XMARK)
  override def avgType(): Array[Avg] = avg.getOrElse(null)
  override def transferedV(originV: Double): Double =
    if (expr != null) expr.compute(originV) else originV
}

case class FunAction(fun: (Double) => Double, avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = fun(originV)
}

case class StrAction(str: String, avg: Option[Array[Avg]] = None) extends Action {
  override def avgType() = avg.getOrElse(null)
  override def transferedV(originV: Double): Double = {
    val toolBox = currentMirror.mkToolBox()
    val tree = toolBox.parse(str.replaceAll(String.valueOf(Actions.XMARK), originV.toString))
    val res = toolBox.eval(tree).asInstanceOf[Double]
    res
  }
}
