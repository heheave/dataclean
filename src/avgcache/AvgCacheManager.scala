package avgcache

import java.util.concurrent._

import org.apache.log4j.Logger
import org.apache.spark.Logging


/**
  * Created by xiaoke on 17-6-2.
  */

class CacheInfo(btime: Long) {

  private var sumD: Double = 0

  private var sumN: Int = 0

  val beginTime = btime

  def add(d: Double) = synchronized {
    sumD += d
    sumN += 1
  }

  def sumData = sumD

  def sumNum = sumN

  override def hashCode(): Int = (beginTime ^ (beginTime >>> 32)).toInt

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: CacheInfo => beginTime == beginTime
    case _ => false
  }
}

case class CacheKey(d: String) {

  val did = d

  override def equals(other: Any): Boolean = other match {
    case that: CacheKey =>
        did == that.did
    case _ => false
  }

  override def hashCode(): Int = {
    if (did == null) 0 else did.hashCode
    //val state = Seq(did)
    //state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

sealed class AvgCacheMap(avg: Avg, fun: Avg => Unit) extends Logging{

  private var scheduledFuture: ScheduledFuture[_] = _

  lazy val avgMap = new ConcurrentHashMap[CacheKey, CacheInfo]()

  def startCheckRunner(excutorPool: ScheduledExecutorService): Unit = {
    val interval = avg.getInterval()
    log.info("-----------------------------: Begin to startCheckRunner")
    scheduledFuture = excutorPool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        val bt = System.currentTimeMillis()
        log.info("-----------------------------: Begin startCheckRunner" + bt)
        try {
          val entryIter = avgMap.entrySet().iterator()
          val curTime = System.currentTimeMillis() - (interval << 1)
          //val curTimeFormated = avg.format(curTime)
          if (entryIter.hasNext) {
            val toRemovedKey = new java.util.HashMap[CacheKey, CacheInfo]()
            while (entryIter.hasNext) {
              val entry = entryIter.next()
              if (entry.getValue != null && curTime - entry.getValue.beginTime > 0) {
                 toRemovedKey.put(entry.getKey, entry.getValue)
              }
            }

            if (!toRemovedKey.isEmpty) {
              val toRmIter = toRemovedKey.entrySet().iterator()
              while (toRmIter.hasNext) {
                val toRmEntry = toRmIter.next()
                avgMap.remove(toRmEntry.getKey, toRmEntry.getValue)
              }
              toRemovedKey.clear()
            }
          }
        } catch {
          case e: Throwable => log.warn("Schedule at " + avg.avgName(), e)
        }
        if (avgMap.isEmpty) {
          fun(avg)
        }
        val et = System.currentTimeMillis()
        log.info("-----------------------------: End startCheckRunner" + (et - bt))
      }
    }, interval, interval << 1, TimeUnit.MILLISECONDS)
  }

  def close() = {
    log.info("-----------------------------: Stop startCheckRunner")
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false)
    }
    avgMap.clear()
  }

  def avgName = avg.avgName

  def isEmpty = avgMap.isEmpty

  def addData(did: String, timestamp: Long, data: Double): CacheInfo = {
    val key = CacheKey(did)
    val cacheInfo = avgMap.get(key)
    val ctime = avg.format(timestamp)
    log.info("-----------------------------ctime: " + ctime)
    if (cacheInfo != null) {
      val btime = cacheInfo.beginTime
      log.info("-----------------------------btime: " + btime)
      if (ctime.equals(btime)) {
        cacheInfo.add(data)
        null
      } else {
        val newCacheInfo = new CacheInfo(ctime)
        newCacheInfo.add(data)
        avgMap.put(key, newCacheInfo)
        cacheInfo
      }
    } else {
      val newCacheInfo = new CacheInfo(ctime)
      newCacheInfo.add(data)
      avgMap.put(key, newCacheInfo)
      null
    }
  }
}

sealed class AvgCacheManager extends Logging{

  lazy val excutorPool = Executors.newScheduledThreadPool(1)

  private val cacheMaps = new ConcurrentHashMap[Avg, AvgCacheMap]

  private val closeFun = (avg: Avg) => cacheMaps.synchronized {
    val getMap = cacheMaps.get(avg)
    if (getMap != null && getMap.isEmpty) {
      val removedMap = cacheMaps.remove(avg)
      if (removedMap != null) {
        removedMap.close()
      }
    }
  }

  def putData(did: String, timestamp: Long, data: Double, avg: Avg): CacheInfo = {
    if (avg != null) {
      val cacheMap = cacheMaps.get(avg)
      if (cacheMap == null) {
        val newCacheMap = new AvgCacheMap(avg, closeFun)
        val oldCacheMap = cacheMaps.synchronized {
          cacheMaps.putIfAbsent(avg, newCacheMap)
        }
        if (oldCacheMap == null) {
          log.info("------------ACM-----------Add New Map: " + avg.avgName())
          newCacheMap.startCheckRunner(excutorPool)
          newCacheMap.addData(did, timestamp, data)
        } else {
          oldCacheMap.addData(did, timestamp, data)
        }
      } else {
        cacheMap.addData(did, timestamp, data)
      }
    } else {
      null
    }
  }

  def close(): Unit = {
    excutorPool.shutdown()
    cacheMaps.clear()
  }
}

class AvgCacheTool(fun: () => AvgCacheManager) extends Serializable{

  lazy val avgCache = fun()

  def addData(did: String, timestamp: Long, data: Double, avg: Avg): CacheInfo = avgCache.putData(did, timestamp, data, avg)
}



object AvgCacheManager {
  def apply(): AvgCacheTool = {
    val f = () => {
      val acm = new AvgCacheManager()
      sys.addShutdownHook(acm.close())
      acm
    }
    new AvgCacheTool(f)
  }
}
