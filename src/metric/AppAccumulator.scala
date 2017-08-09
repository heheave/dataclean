package metric

import java.util

import org.apache.spark.{Logging, AccumulatorParam}

/**
  * Created by xiaoke on 17-8-9.
  */

class AppAccumulatorInfo(val app: String, var mn: Long, var vp: Long, var ip: Long, var cp: Long, var up: Long) extends Serializable{
  def addWithDI(di: (String, Long, Long, Long, Long, Long)) = {
    assert(app == di._1)
    mn += di._2
    vp += di._3
    ip += di._4
    cp += di._5
    up += di._6
  }

  def addWithAAI(aai: AppAccumulatorInfo) = {
    assert(app == aai.app)
    mn += aai.mn
    vp += aai.vp
    ip += aai.ip
    cp += aai.cp
    up += aai.up
  }
}

object AppAccumulatorInfo {
  def apply(app: String, mn: Long, vp: Long, ip: Long, cp: Long, up: Long) = {
    new AppAccumulatorInfo(app, mn, vp, ip, cp, up)
  }
}

class AppAccumulator extends AccumulatorParam[util.List[AppAccumulatorInfo]] with Logging{

  override def addInPlace(r1: util.List[AppAccumulatorInfo], r2: util.List[AppAccumulatorInfo]): util.List[AppAccumulatorInfo] = {
    if (r2 == null) {
      r1
    } else {
      val r2Iter = r2.iterator()
      while (r2Iter.hasNext) {
        val r2Value = r2Iter.next()
        var idx = 0
        var hasApp = false
        while (idx < r1.size() && hasApp) {
          val r1Value = r1.get(idx)
          if (r1Value.app.equals(r2Value.app)) {
            r1Value.addWithAAI(r2Value)
            hasApp = true
          }
          idx += 1
        }
        if (!hasApp) {
          r1.synchronized(r1.add(r2Value))
        }
      }
      r1
    }
  }

  override def zero(initialValue: util.List[AppAccumulatorInfo]): util.List[AppAccumulatorInfo] = {
    if (initialValue == null) {
      new util.ArrayList[AppAccumulatorInfo]()
    } else {
      initialValue
    }
  }
}
