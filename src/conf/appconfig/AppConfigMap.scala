package conf.appconfig

import java.util

/**
  * Created by xiaoke on 17-7-23.
  */
// for app config
case class AppEntry(id: Int,
                    appname: String,
                    mongourl: String,
                    mongoport: Int,
                    apidatatopic: String,
                    apiavgtopic: String,
                    otherid: Int) {
}

class AppConfigMap {

  private val configMap = new util.HashMap[String, AppEntry]

  def get(appName: String) = configMap.synchronized {
    configMap.get(appName)
  }

  def set(appName: String, entry: AppEntry) = configMap.synchronized {
    configMap.put(appName, entry)
  }

  def remove(appName: String) = configMap.synchronized {
    configMap.remove(appName)
  }

  def clear() = configMap.synchronized {
    configMap.clear()
  }
}
