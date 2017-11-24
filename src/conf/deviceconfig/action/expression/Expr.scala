package conf.deviceconfig.action.expression

import net.sf.json.JSONObject

/**
  * Created by xiaoke on 17-6-21.
  */
trait Expr {
  def compute(v: JSONObject): Double
}

case class ConstantExpr(const: Double) extends Expr{
  override def compute(v: JSONObject): Double = const
  override def toString(): String = const.toString
}

case class VariableExpr(key: String) extends Expr{
  override def compute(v: JSONObject): Double = {
    if (v.containsKey(key)) {
      v.getDouble(key)
    } else {
      0.0
    }
  }
  override def toString(): String = key
}

case class AddExpr(e1: Expr, e2: Expr) extends Expr {
  override def compute(v: JSONObject): Double = e1.compute(v) + e2.compute(v)
  override def toString(): String = "(%s + %s)".format(e1.toString, e2.toString)
}

case class SubExpr(e1: Expr, e2: Expr) extends Expr {
  override def compute(v: JSONObject): Double = e1.compute(v) - e2.compute(v)
  override def toString(): String = "(%s - %s)".format(e1.toString, e2.toString)
}

case class MulExpr(e1: Expr, e2: Expr) extends Expr {
  override def compute(v: JSONObject): Double = e1.compute(v) * e2.compute(v)
  override def toString(): String = "%s * %s".format(e1.toString, e2.toString)
}

case class DivExpr(e1: Expr, e2: Expr) extends Expr {
  override def compute(v: JSONObject): Double = e1.compute(v) / e2.compute(v)
  override def toString(): String = "%s / %s".format(e1.toString, e2.toString)
}
