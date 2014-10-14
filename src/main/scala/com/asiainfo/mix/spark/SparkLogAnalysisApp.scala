package com.asiainfo.mix.spark

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.asiainfo.mix.spark.impl.MergeLogAnalysis
import org.apache.hadoop.conf.Configuration
import java.io.FileWriter

/**
 * @author 宿荣全
 * @since 2014.09.28
 * @comment
 * 功能：
 * 实时监控配置文件指定的HDFS路径，把该路径下以及子目录下的文件转换成RDD，进行spark处理。
 */
object SparkLogAnalysisApp extends MixLog {

  /**
   * properiesMap:applacation properies 配置<br>
   * HDFSfilePathMap:输入日志文件类型以及路径(HDFS)配置<br>
   * dbSourceMap:db(mysql) 驱动配置<br>
   * logStructMap：log 日志属性配置<br>
   * tablesDefMap：mysql表定义配置<br>
   */
  def main(args: Array[String]): Unit = {
    try {
      
    }catch {
      case e: Exception => println(this.getClass().getName() + ".fileScanner:Exception!"); e.printStackTrace();
    }

    // xml解析
    XmlProperiesAnalysis.getXmlProperies
    val properties = XmlProperiesAnalysis.xmlProperiesAnalysis

    val properiesMap = properties._1
    val appName = properiesMap("appName")
    // 一个job最大的文件数
    val unionRDD_count = (properiesMap("unionRDD_count")).toInt
    // 本地文件日志
    val locallogpath = properiesMap("localLogDir")

    // TODO local test 用
    // val conf = new SparkConf().setAppName(appName).setMaster("local[2]")
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    //  加载rddfile历史记录，rdd历史记录文件宁成的hash树
    val historyInfo = Util.historyRddAnalysis(locallogpath)
    // 最新历史记录文件路径
    val historyFile = historyInfo._1
    // 最新历史记录文件拧成的hash路径树
    val historyrddMap = historyInfo._2
    println("历史处理文件加载[INFO:]" + historyFile)

    // 去除历史记录文件中HDFS文件系统中已不存在的文件
    Util.del_Hdfs_NotExist(historyrddMap)
    // 重新生成历史记录文件列表
    val historyRddWriter = Util.rddFileHistoryWriter(locallogpath)
    // 已有历史记录写入新的记录
    historyrddMap.map(f => {
      f._2.foreach(each => {
        historyRddWriter.write(each + System.getProperty("line.separator"))
        historyRddWriter.flush
      })
    })
    println(this.getClass().getName() + "[INFO:]" + "spark batch最新处理历史记录已更新完成！")

    // 开启配置文件中所有日志类型的文件监控
    val HDFSfilePathMap = properties._2
    // 所有类弄日志文件封装成rdd后，存入rddQueue等待调用执行
    val rddQueue = new LinkedBlockingQueue[(String, RDD[String])]()
    HDFSfilePathMap.foreach(f => {
      val topicLogType = f._1
      val dir = f._2("dir")
      val loopInterval = (f._2("loopInterval")).toLong
      val lbqHdfsFile = new LinkedBlockingQueue[String]()
      // 开启文件监控线程
      new Thread(new FileMonitor(historyrddMap, f._2("topicLogType"), locallogpath, dir, loopInterval, lbqHdfsFile)).start()
      // 开启文件名－>rdd的转换线程
      new Thread(new FileToRDD(rddQueue, sc, topicLogType, lbqHdfsFile)).start()
    })
    println(this.getClass().getName() + "[INFO:]" + "spark batch初期准备全部完成,等待执行任务！")
    while (true) {
      var historyRddFile = ""
      var rddLinks = IndexedSeq[RDD[String]]()
      if (rddQueue.size() >= unionRDD_count) {
        rddLinks = 1 to unionRDD_count map (f => {
          val frdd = rddQueue.take
          // 已处理过的文件要做备份，以备宕机，重新加载时不被重复
          historyRddFile = historyRddFile + frdd._1 + System.getProperty("line.separator")
          frdd._2
        })
      } else if (rddQueue.size() > 0) {
        rddLinks = 1 to rddQueue.size() map (f => {
          val frdd = rddQueue.take
          // 已处理过的文件要做备份，以备宕机，重新加载时不被重复
          historyRddFile = historyRddFile + frdd._1 + System.getProperty("line.separator")
          frdd._2
        })
      } else {
        val rdd = rddQueue.take
        rddLinks = rddLinks :+ rdd._2
        // 已处理过的文件要做备份，以备宕机，重新加载时不被重复
        historyRddFile = historyRddFile + rdd._1 + System.getProperty("line.separator")
      }
      val rddlink = rddLinks.reduce(_.union(_))
      executor(rddlink, historyRddWriter, historyRddFile)
    }
  }

  /**
   * 执行job并保存已完成的RDD文件
   */
  def executor(rddlink: RDD[String], writer: FileWriter, text: String) {
    val job = new MergeLogAnalysis(rddlink).merge
    job.count
    writer.write(text)
    writer.flush
  }
}