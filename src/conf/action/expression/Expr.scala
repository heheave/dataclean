package conf.action.expression

/**
  * Created by xiaoke on 17-6-21.
  */
trait Expr {
  def compute(v: Double): Double
}

case class ConstantExpr(const: Double) extends Expr{
  override def compute(v: Double): Double = const
  override def toString(): String = const.toString
}

case class VariableExpr() extends Expr{
  override def compute(v: Double): Double = v
  override def toString(): String = "?"
}

case class AddExpr(e1: Expr, e2: Expr) extends Expr {
  override def compute(v: Double): Double = e1.compute(v) + e2.compute(v)
  override def toString(): String = "(%s + %s)".format(e1.toString, e2.toString)
}

case class SubExpr(e1: Expr, e2: Expr) extends Expr {
  override def compute(v: Double): Double = e1.compute(v) - e2.compute(v)
  override def toString(): String = "(%s - %s)".format(e1.toString, e2.toString)
}

case class MulExpr(e1: Expr, e2: Expr) extends Expr {
  override def compute(v: Double): Double = e1.compute(v) * e2.compute(v)
  override def toString(): String = "%s * %s".format(e1.toString, e2.toString)
}

case class DivExpr(e1: Expr, e2: Expr) extends Expr {
  override def compute(v: Double): Double = e1.compute(v) / e2.compute(v)
  override def toString(): String = "%s / %s".format(e1.toString, e2.toString)
}
