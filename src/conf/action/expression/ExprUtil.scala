package conf.action.expression

import java.util

import org.apache.spark.Logging

/**
  * Created by xiaoke on 17-6-21.
  */
object ExprUtil extends Logging{
  def fromString(str: String, XMark: Char): Expr = {
    try {
      val stack = new util.Stack[Expr]
      val list = toReversePolish(str, XMark)
      val iter = list.iterator()
      while (iter.hasNext) {
        val elem = iter.next
        if (elem.length == 1 && isOpChar(elem.charAt(0))) {
          if (stack.size() > 1) {
            val e2 = stack.pop()
            val e1 = stack.pop()
            stack.push(ExprFactory.getBinocularExpr(e1, e2, elem.charAt(0)))
          } else {
            throw new IllegalArgumentException("Could not compile expr: " + str)
          }
        } else {
          val expr = if (XMark == elem.charAt(0)) {
            ExprFactory.variableExpr
          } else {
            ExprFactory.getConstantExpr(elem.toDouble)
          }
          stack.push(expr)
        }
      }
      if (stack.size() == 1) {
        stack.pop()
      } else {
        throw new IllegalArgumentException("Could not compile expr: " + str)
      }
    } catch {
      case e: Throwable => {
        log.warn(e.getMessage, e)
        null
      }
    }
  }

  private def toReversePolish(str: String, mark: Char): util.List[String] = {
    val result = new util.LinkedList[String]()
    val stack = new util.Stack[Char]()
    val tmpNumSb = new StringBuffer()
    for (c <- str) {
      if (c != ' ') {
        if ((c >= '0' && c <= '9') || c == '.') {
          tmpNumSb.append(c)
        } else if (c == mark) {
          result.add(String.valueOf(c))
        } else {
          if (tmpNumSb.length() > 0) {
            result.add(tmpNumSb.toString)
            tmpNumSb.setLength(0)
          }

          if (c == '(') {
            stack.push(c)
          } else if (c == ')') {
            var tmpC: Char = 0
            while (!stack.empty() && tmpC != '(') {
              tmpC = stack.pop()
              if (tmpC != '(') {
                result.add(String.valueOf(tmpC))
              }
            }
            if (tmpC != '(') {
              throw new IllegalArgumentException("Could not compile expr: " + str)
            }
          } else if (isOpChar(c)) {
            while (!stack.empty() && isLowerThan(c, stack.peek())) {
              val tmpC = stack.pop()
              result.add(String.valueOf(tmpC))
            }
            stack.push(c)
          } else {
            throw new IllegalArgumentException("Could not compile expr: " + str)
          }
        }
      }
    }
    if (tmpNumSb.length() > 0) {
      result.add(tmpNumSb.toString)
    }
    while (!stack.isEmpty) {
      val tmpC = stack.pop()
      if (tmpC == '(') {
        throw new IllegalArgumentException("Could not compile expr: " + str)
      } else {
        result.add(String.valueOf(tmpC))
      }
    }
    result
  }

  private def isOpChar(opChar: Char) = {
    opChar match {
      case '+' => true
      case '-' => true
      case '*' => true
      case '/' => true
      case _ => false
    }
  }

  private def isLowerThan(ch1: Char, ch2: Char): Boolean = {
    def level(ch: Char) = ch match {
      case '(' => 0
      case '+' => 1
      case '-' => 1
      case '*' => 2
      case '/' => 2
      case _ =>  throw new IllegalArgumentException(s"Unsupported comparison between: ${ch1} and ${ch2}")
    }
    level(ch1) <= level(ch2)
  }
}
