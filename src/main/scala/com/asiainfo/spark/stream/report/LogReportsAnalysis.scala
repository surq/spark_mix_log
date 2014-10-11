package com.asiainfo.spark.stream.report

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer
import java.io.{ File, FileWriter }
import java.io.IOException
import java.util.Calendar

/**
 * @since 2014.08.12
 * @comment 在指定的时间问隔内打印一次报表，若在一个周期内数据达到5000条则也会写报表<br>
 * @param args(监控topic，监控log打印报表周期（m分钟），监控log生成路径)<br>
 * lbq:从流中收到的消息<br>
 */
class LogReportsAnalysis(args: Array[String], lbq: LinkedBlockingQueue[String], lbqHdfsFile: LinkedBlockingQueue[String]) extends Runnable {

  val Array(reporttopic, fileLineSize, logpath, hdfsPaht) = args
  val reportFileName = logpath + System.getProperty("file.separator") + "dstreamScanLog.log"
  val cacheFileName = logpath + System.getProperty("file.separator") + "cacheRecords.log"
  //　报表文件详细信息
  var reportInfoList = ArrayBuffer[String]()

  //打印报表的时间间隔
  val logFile = new File(logpath)
  override def run() {
    var cacheFileexist_flg = true
    while (true) {
      // 清除本机以存在的以往的落地缓存数据
      if (cacheFileexist_flg){
        cacheFileexist_flg = false
        delFile(new File(cacheFileName))
      }
      val reportbean = lbq.take()
      reportInfoList += reportbean
      // 缓存数据落地
      writeCacheRecord(reportbean)
      if (reportInfoList.size == fileLineSize.toInt) {
        outReport
        // 生成hdfs文件并上传后，清除本地的落地缓存数据文件
        delFile(new File(cacheFileName))
      }
    }
  }

  /**
   * 5000条一个hdfs文件,若不够则存在本地硬盘<br>
   * 把缓存中的记录写到本地硬盘<br>
   */
  def writeCacheRecord(recode: String) {
    try {
      if (!logFile.isDirectory()) {
        logFile.mkdirs()
      }
      val writer = new FileWriter(cacheFileName, true)
      writer.write(recode + System.getProperty("line.separator"))
      writer.flush
      writer.close
    } catch {
      case e: IOException => e.printStackTrace()
      case ex: Exception => ex.printStackTrace()
    }
  }

  /**
   * 删除文件
   */
  def delFile(file:File) = if (file.exists) file.delete
  
  /**
   * 打印报表
   */
  def outReport {

    try {
      if (!logFile.isDirectory()) {
        logFile.mkdirs()
      }
      if (reportInfoList.size > 0) {
        writ
        reportInfoList.clear
      }
    } catch {
      case e: IOException => e.printStackTrace()
      case ex: Exception => ex.printStackTrace()
    }
  }

  /**
   * 写报表文件
   */
  def writ {
    val writer = new FileWriter(reportFileName, false)
    reportInfoList.foreach(f => {
      writer.write(f + System.getProperty("line.separator"))
      writer.flush
    })
    writer.close
    val hdfsFileName = LogReportUtil.modifyFileName(reportFileName)
    if (hdfsFileName != null) {
      println("[INFO:] created local scan-File：" + hdfsFileName)
      // 上传hdfs
      lbqHdfsFile.offer(hdfsFileName)
    }
  }
}