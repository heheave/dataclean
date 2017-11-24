package conf

import java.net.InetSocketAddress
import java.util
import java.util.Properties
import javaclz.cache.cacheClient.LocalCacheService
import javaclz.cache.de.{DE, DEDeserEntrier}

import conf.deviceconfig.action.Action

/**
  * Created by xiaoke on 17-11-24.
  */
class DECacheManager(prop: Properties) {

  private val cacheType = prop.getProperty("cachetype", "deviceconf")
  private val cacheServerHost = prop.getProperty("cacheServerHost", "localhost")
  private val cacheServerPort = prop.getProperty("cacheServerPort", "8888")
  private val cacheMQHost = prop.getProperty("cacheMQHost", "localhost")
  private val cacheMQPort = prop.getProperty("cacheMQPort", "5672")
  private val cacheMQTopic = prop.getProperty("cacheMQTopic", "deviceconf")
  private val cacheLRUSize = prop.getProperty("cacheLRUSize", "1000")

  val localCache = new LocalCacheService[DEDeserEntrier](
    cacheType,
    new InetSocketAddress(cacheServerHost, cacheServerPort.toInt),
    cacheMQHost,
    cacheMQPort.toInt,
    cacheMQTopic,
    cacheLRUSize.toInt,
    new DEDeserEntrier()
  )

  def start(): Unit = {
    localCache.startService()
  }

  def stop(): Unit = {
    localCache.stopService()
  }

  def getEntry(key: String): util.List[DE] = {
    val centry = localCache.get(key)
    if (centry != null && centry.getAttach != null) {
      centry.getAttach.asInstanceOf[util.List[DE]]
    } else {
      null
    }
  }
}

class DECacheManagerSink(fun: () => DECacheManager) extends Serializable{

  private lazy val deviceConfig = fun()

  //def configMap(app: String) = deviceConfig.appConfigMap(app)

  //def appConfig(appName: String) = deviceConfig.getAppConf(appName)
  def actionByDcode(dcode: String): util.List[DE] = {
    deviceConfig.getEntry(dcode)
  }
}

object DECacheManagerSink {
  def apply(prop: Properties): DECacheManagerSink  = {
    val f = () => {
      val res= new DECacheManager(prop)
      res.start()
      sys.addShutdownHook(() => {res.stop()})
      res
    }
    new DECacheManagerSink(f)
  }
}

