package com.asiainfo.mix.spark

import java.io.FileWriter
import java.util.Calendar
import java.io.File
import java.io.IOException
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * @author 宿荣全
 * @since 2014.09.29
 */
object Util {

  val dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss")

  /**
   * 把rdd历史记录文件宁成hash树，供初始化加载新文件时，刨除已经处理过的文件<br>
   * 返回历史文件路径和拧成的路径树<br>
   */
  def historyRddAnalysis(locallogpath: String): (String, Map[String, ArrayBuffer[String]]) = {

    val historyFileList = new File(locallogpath)
    // 装载rdd历史记录
    val RDDHistoryMap = Map[String, ArrayBuffer[String]]()
    var historyFileName = ""
    // 找出最新的历史rrdFile 列表,滤掉空文件
    val files = (historyFileList.listFiles).filter(_.isFile).filter(_.getName().matches("""^(historyFileRdd_)[0-9]{14}(\.log)$"""))
      .filter(_.length() != 0).map(_.getAbsoluteFile().toString())
    if (files.size > 0) {
      //最新的历史记录文件
      historyFileName = files.max
      var source = Source.fromFile(historyFileName)
      for (line <- source.getLines) {
        // 取文件夹为key
        val key = line.substring(0, line.lastIndexOf(System.getProperty("file.separator")))
        var rddlist: ArrayBuffer[String] = null
        if (RDDHistoryMap.getOrElse(key, null) == null) {
          rddlist = ArrayBuffer[String]()
        } else {
          rddlist = RDDHistoryMap(key)
        }
        // 当前文件夹下的所有文件列表
        rddlist += line
        RDDHistoryMap += (key -> rddlist)
      }
    }
    (historyFileName, RDDHistoryMap)
  }

  /**
   * 去除HDFS系统已经不存在的文件
   */
  def del_Hdfs_NotExist(historyMap: Map[String, ArrayBuffer[String]]) = {
    val conf = new Configuration()
    //local test 用
    //  conf.addResource(new Path("conf/core-site.xml"))
    val HDFSFileSytem = FileSystem.get(conf)
    historyMap.foreach(f => {
      // 文件夹目录不存在，则其下的所有文件都要移除;否则扁历目录把不存在刨除
      if (!HDFSFileSytem.exists(new Path(f._1))) historyMap -= f._1 else historyMap += (f._1 -> (for (eachfile <- f._2 if (HDFSFileSytem.exists(new Path(eachfile)))) yield (eachfile)))
    })
  }

  /**
   * 创建日志写文件，写入器FileWriter
   * 在指定的路径locallogpath下生成topicLogType文件夹，并生成以时间为名称的日志文件。
   */
  def mixLogWriter(locallogpath: String, topicLogType: String): FileWriter = {
    try {
      val logdir = locallogpath + System.getProperty("file.separator") + topicLogType + System.getProperty("file.separator")
      val logfile = logdir + getNowTime + ".log"
      val logfolder = new File(logdir)
      if (!logfolder.isDirectory()) logfolder.mkdirs()
      new FileWriter(logfile, true)
    } catch {
      case ie: IOException => { ie.printStackTrace(); null }
      case e: IOException => { e.printStackTrace(); null }
    }
  }

  /**
   * 输入文件路径，和文件名生成以此文件名为前缀，historyFileRdd+时间为后缀的文件读写器
   */
  def rddFileHistoryWriter(locallogpath: String): FileWriter = {
    try {
      val logFile = locallogpath + System.getProperty("file.separator") + "historyFileRdd"
      val logFileName = logFile + "_" + getNowTime + ".log"

      val logfolder = new File(locallogpath)
      if (!logfolder.isDirectory()) logfolder.mkdirs()
      new FileWriter(logFileName, true)

    } catch {
      case ie: IOException => { ie.printStackTrace(); null }
      case e: IOException => { e.printStackTrace(); null }
    }
  }

  /**
   * 获取当前时间
   */
  def getNowTime = dateFormat.format(Calendar.getInstance().getTimeInMillis())
}