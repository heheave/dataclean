package metric

import java.util

import org.apache.spark.{Logging, AccumulatorParam}

/**
  * Created by xiaoke on 17-8-9.
  */

class AppAccumulatorInfo(val id: Int, var mn: Long, var mb: Long) extends Serializable{
  def addWithDI(di: (Int, Long, Long)) = {
    assert(id == di._1)
    mn += di._2
    mb += di._3
  }

  def addWithAAI(aai: AppAccumulatorInfo) = {
    assert(id == aai.id)
    mn += aai.mn
    mb += aai.mb
  }
}

object AppAccumulatorInfo {
  def apply(app: Int, mn: Long, mb: Long) = {
    new AppAccumulatorInfo(app, mn, mb)
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
          if (r1Value.id.equals(r2Value.id)) {
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
