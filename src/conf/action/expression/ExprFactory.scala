package conf.action.expression

/**
  * Created by xiaoke on 17-6-21.
  */
object ExprFactory {

  val variableExpr = VariableExpr()

  def getConstantExpr(const: Double) = ConstantExpr(const)

  def getBinocularExpr(e1: Expr, e2: Expr, opChar: Char) = opChar match {
    case '+' => AddExpr(e1, e2)
    case '-' => SubExpr(e1, e2)
    case '*' => MulExpr(e1, e2)
    case '/' => DivExpr(e1, e2)
    case _ => throw new IllegalArgumentException("Unsupported operation: " + opChar)
  }
}
