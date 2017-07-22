package metric

import java.util.concurrent.atomic.{AtomicLong}

/**
  * Created by xiaoke on 17-7-17.
  */
object MessageMetric {

  private val validStatistics = new AtomicLong(0)

  private val invalidStatistics = new AtomicLong(0)


  def validSet(num: Long) = {
    val oldV = validStatistics.get()
    if (oldV < num) {
      validStatistics.set(num)
    }
  }

  def validIncrease(num: Long) = {
    validStatistics.addAndGet(num)
  }

  def invalidSet(num: Long) = {
    val oldV = invalidStatistics.get()
    if (oldV < num) {
      invalidStatistics.set(num)
    }
  }

  def invalidIncrease(num: Long) = {
    invalidStatistics.addAndGet(num)
  }

  def validNum = validStatistics.get()

  def invalidNum = invalidStatistics.get()
}
