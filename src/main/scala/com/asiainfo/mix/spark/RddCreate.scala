package com.asiainfo.mix.spark

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.Logging
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.dstream.DStream
import scala.beans.BeanProperty
import com.asiainfo.spark.stream.report.LogReportUtil
import java.util.Calendar
import com.asiainfo.mix.streaming_log.LogTools
import org.apache.spark._
import com.asiainfo.mix.streaming_log.StreamAction
import org.apache.spark.rdd.RDD
/**
 * @author 宿荣全
 * @since 2014.09.26
 * 把指定的文件转找成rdd并返回rddQueue<br>
 */
class RddCreate extends Serializable with MixLog{

  /**
   * @param topicLogType:日志类型
   * @param textFile:HDFS文件路径
   * 功能介绍：
   * 把各类型的日志转换成rdd，做字段个数check<br>
   * 然后根把提供的日志类型，创建对应的rdd处理流程<br>
   */
  def create(sc: SparkContext, topicLogType: String, textFile: String): RDD[String] = {

    val value = XmlProperiesAnalysis.xmlProperiesAnalysis
    //输入日志文件类型以及路径(HDFS)配置
    val HDFSfilePathMap = value._2
    // log 日志属性配置
    val logStructMap = value._4

    // 日志文件类弄RDD生成器设定
    val hdfsPropertiesMap = HDFSfilePathMap(topicLogType)
    val appClass = hdfsPropertiesMap("appClass")
    // 日志文件字段分隔符设定、并取log结构
    val logpropertiesMap = logStructMap(topicLogType)
    val items = logpropertiesMap("items")
    var separator = logpropertiesMap("separator")
    val ox002: Char = 2
    if (separator == "") separator = ox002.toString

    val stream = sc.textFile(textFile)
    val inputStream: RDD[Array[(String, String)]] = stream.map(f => {
      // 追加“MixSparkLogEndSeparator"目的是为split操作时字段长度保持不变，最后的“”不被省掉
      var data = f + separator + "MixSparkLogEndSeparator"
      val recodeList = data.split(separator)
      val recode = (for { index <- 0 until recodeList.size - 1 } yield (recodeList(index))).toArray
      val keyValueArray = (items.split(",")).zip(recode)
      keyValueArray
    }).filter(f => {
      val temMap = f.toMap
      // 有误的log日志数据
      if (temMap("log_length").trim != (temMap.size).toString) {
//        printWranning(this.getClass(),"日志数据字段个数有误［实际字段数=" + temMap.size + "]" + f.mkString(","))
        false
      } else {
        true
      }
    })

    val clz = Class.forName(appClass)
    val constructors = clz.getConstructors()
    val constructor = constructors(0).newInstance()
    constructor.asInstanceOf[BatchStream].run(topicLogType, inputStream)
  }
}