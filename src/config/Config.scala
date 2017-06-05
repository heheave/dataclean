package config

import java.util

import action.Action

/**
  * Created by xiaoke on 17-6-1.
  */
class Config extends Serializable{

  private val actionsMap = new util.HashMap[String, util.Map[Int, Action]]()

  def getActions(did: String, pidx: Int): Action = {
    val deviceActions = actionsMap.get(did)
    if (deviceActions != null) {
      val tmpAction = if (pidx >= 0) {
        deviceActions.get(pidx)
      } else {
        null
      }
      if (tmpAction == null) {
        deviceActions.get(-1)
      } else {
        tmpAction
      }
    } else {
      null
    }
  }

  def append(did: String, pidx: Int, action: Action): Unit = {
    val addPidx = if (pidx < 0) -1 else pidx
    val deviceActions = actionsMap.get(did)
    if (deviceActions != null) {
      deviceActions.put(addPidx, action)
    } else {
      val newActionsMap = new util.HashMap[Int, Action]()
      newActionsMap.put(addPidx, action)
      actionsMap.put(did, newActionsMap)
    }
  }

  def append(did: String, action: Action): Unit = {
    append(did, -1, action);
  }

  def remove(did: String, pidx: Int): Unit = {
    val delPidx = if (pidx < 0) -1 else pidx
    val deviceActions = actionsMap.get(did)
    if (deviceActions != null) {
      deviceActions.remove(delPidx)
    }
  }

  def remove(did: String): Unit = {
    actionsMap.remove(did)
  }

  def clear(): Unit = {
    actionsMap.clear()
  }

  def updateByConfig(old: util.HashSet[String], newConfig: Config): Unit = synchronized {
    if (actionsMap.isEmpty) {
      actionsMap.putAll(newConfig.actionsMap)
    } else {
      val iter = old.iterator()
      while (iter.hasNext) {
        val did = iter.next()
        newConfig.actionsMap.put(did, actionsMap.get(did))
      }
      clear()
      actionsMap.putAll(newConfig.actionsMap)
      newConfig.clear()
    }
  }
}
