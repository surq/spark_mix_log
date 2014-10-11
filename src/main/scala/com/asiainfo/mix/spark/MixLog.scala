package com.asiainfo.mix.spark

import com.asiainfo.mix.streaming_log.LogTools

trait MixLog extends Serializable {
  def printInfo(className: Class[_], msg: String) {
    LogTools.mixInfo("["+ className.getName() +"] INFO: " + msg)
  }
  def printDebug(className: Class[_], msg: String) {
    LogTools.mixDebug("["+ className.getName() +"] DEBUG: " + msg)
  }
  def printWranning(className: Class[_], msg: String) {
    LogTools.mixWranning("["+ className.getName() +"] WRANNING: " + msg)
  }
  def printError(className: Class[_], msg: String) {
    LogTools.mixError("["+ className.getName() +"] ERROR: " + msg)
  }
  def printError(className: Class[_], msg: String,U:Unit) {
    LogTools.mixError("["+ className.getName() +"] ERROR: " + msg ,U)
  }
}