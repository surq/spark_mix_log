package com.asiainfo.mix.streaming_log

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.Logging
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.dstream.DStream
import scala.beans.BeanProperty

/**
 * @author surq
 * @since 2014.07.14
 * 功能解介：<br>
 * 1、mix Log 处理入口类<br>
 * 2、启动参为kafa的topic名字<br>
 * 3、根据传入的kafa　topic名字<br>
 *   解析logconf.xml中dataSource和logProperties部分<br>
 *   dataSource:为接收kafka数据的配置部分<br>
 *   logProperties:为欲处理log的相关信息<br>
 * 4、调用相应的topic流处理app<br>
 */
object LogAnalysisApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println("Usage: <topic>")
      return
    }

    val xmlFile = XML.load("conf/logconf.xml")
    val kafka = xmlFile \ "kafkaTopics" \ "kafka"

    val topic_parm = args(0).trim
    val kafkaTopics = kafka.filter(p => {
      val topic = (p \ "topic").text.toString.trim
      if (topic == topic_parm) true else false
    })
    if (kafkaTopics.size > 1) {
      printError("logconf.xml: configuration/dataSource/kafka结点中有多个topic是" + topic_parm)
      System.exit(1)
    } else if (kafkaTopics.size < 1) {
      printError("logconf.xml: configuration/dataSource/kafkaa结点中不存在topic是" + topic_parm)
      System.exit(1)
    }

    //------------------kafaka-----------------------------------------
    val consumerNum = (kafkaTopics \ "consumerNum").text.toString.trim
    val appName = (kafkaTopics \ "appName").text.toString.trim
    val zkQuorum = (kafkaTopics \ "zkQuorum").text.toString.trim
    val topic = (kafkaTopics \ "topic").text.toString.trim
    val appClass = (kafkaTopics \ "appClass").text.toString.trim
    val streamSpace = (kafkaTopics \ "streamSpace").text.toString.trim
    var separator = (kafkaTopics \ "separator").text.toString
    val describe = (kafkaTopics \ "describe").text.toString.trim
    // kafka流数据字段分隔符，空时黙认为［0x02］
    val ox002: Char = 2
    if (separator == "") separator = ox002.toString
    //-----------------kafakaOut 配置------------------------------------
    val kafakaOut = xmlFile \ "dataSource" \ "kafakaOut"
    val out_topic = (kafakaOut \ "topic").text.toString.trim
    val out_brokers = (kafakaOut \ "brokers").text.toString.trim
    val out_separator = (kafakaOut \ "separator").text.toString
    val out_describe = (kafakaOut \ "describe").text.toString.trim

    val kafaArray = ArrayBuffer[(String, String)]()
    kafaArray += ("topic" -> out_topic)
    kafaArray += ("brokers" -> out_brokers)
    kafaArray += ("separator" -> out_separator)
    kafaArray += ("describe" -> out_describe)

    //-----------------db(mysql) 配置--------------------------------------
    val dbconf = xmlFile \ "dataSource" \ "dbSource"
    val driver = (dbconf \ "driver").text.toString.trim
    val url = (dbconf \ "url").text.toString.trim
    val user = (dbconf \ "user").text.toString
    val password = (dbconf \ "password").text.toString.trim

    val dbSourceArray = ArrayBuffer[(String, String)]()
    dbSourceArray += ("driver" -> driver)
    dbSourceArray += ("url" -> url)
    dbSourceArray += ("user" -> user)
    dbSourceArray += ("password" -> password)

    //-----------------log 日志属性配置--------------------------------------
    val mixlogs = xmlFile \ "logProperties" \ "log"
    val mixlog = mixlogs.filter(p => {
      val topicLogType = (p \ "topicLogType").text.toString.trim
      if (topicLogType == topic_parm) true else false
    })

    if (mixlog.size > 1) {
      printError("logconf.xml: configuration/logProperties/log 结点中有多个topic是" + topic_parm)
      return
    } else if (mixlog.size < 1) {
      printError("logconf.xml: configuration/logProperties/log 结点中不存在topic是" + topic_parm)
      return
    }

    val mixLogArray = ArrayBuffer[(String, String)]()
    val topicLogType = (mixlog \ "topicLogType").text.toString.trim()
    val logSpace = (mixlog \ "logSpace").text.toString.trim()
    val items = (mixlog \ "items").text.toString.trim()
    val itemsDescribe = (mixlog \ "itemsDescribe").text.toString.trim()
    val rowKey = (mixlog \ "rowKey").text.toString.trim()
    val describe_log = (mixlog \ "describe").text.toString.trim()

    mixLogArray += ("topicLogType" -> topicLogType)
    mixLogArray += ("logSpace" -> logSpace)
    mixLogArray += ("items" -> items)
    mixLogArray += ("itemsDescribe" -> itemsDescribe)
    mixLogArray += ("rowKey" -> rowKey)
    mixLogArray += ("describe" -> describe_log)

    //-----------------更新mysql表配置--------------------------------------    
    val tables = xmlFile \ "tableDefines" \ "table"
    val tableName = (tables \ "tableName").text.toString.trim()
    val tb_items = (tables \ "items").text.toString.trim()
    val tb_itemsDescribe = (tables \ "itemsDescribe").text.toString.trim()
    val tb_describe = (tables \ "describe").text.toString.trim()
    val tablesArray = ArrayBuffer[(String, String)]()

    tablesArray += ("tableName" -> tableName)
    tablesArray += ("items" -> tb_items)
    tablesArray += ("itemsDescribe" -> tb_itemsDescribe)
    tablesArray += ("describe" -> tb_describe)

    // TODO
    //    val master = "local[2]"
    //    val ssc = new StreamingContext(master, appName, Seconds(streamSpace.toInt), System.getenv("SPARK_HOME"))

    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(streamSpace.toInt))
    val stream = (1 to consumerNum.toInt).map(_ => KafkaUtils.createStream(ssc, zkQuorum, "test-consumer-group", Map(topic -> 1)).asInstanceOf[DStream[(String, String)]]).reduce(_.union(_))
    val inputStream: DStream[Array[(String, String)]] = stream.map(f => {
      var data = f._2 + separator + "MixSparkLogEndSeparator"
      val recodeList = data.split(separator)
      val recode = (for { index <- 0 until recodeList.size - 1 } yield (recodeList(index))).toArray
      val keyValueArray = (items.split(",")).zip(recode)
      printDebug("from topic:" + topic + " record: " + keyValueArray.mkString(","))
      keyValueArray
    }).filter(f => {
      val temMap = f.toMap
      // 有误的log日志数据
      if (topic != "merge" && temMap("log_length").trim != (temMap.size).toString) {
        printWRANNING("日志数据字段个数有误［实际字段数=" + temMap.size + "]" + f.mkString(","))
        false
      } else true
    })
    val clz = Class.forName(appClass)
    val constructors = clz.getConstructors()
    val constructor = constructors(0).newInstance()

    val outputStream = constructor.asInstanceOf[StreamAction].run(inputStream, Seq(kafaArray.toArray, dbSourceArray.toArray, mixLogArray.toArray, tablesArray.toArray))
    outputStream.saveAsTextFiles("/spark_log/sourcelog/" + topic + "/" + topic, "log")
    ssc.start()
    ssc.awaitTermination()
  }

  def printInfo(msg: String) {
    LogTools.mixInfo("[com.asiainfo.mix.streaming_log.LogAnalysisApp] INFO: " + msg)
  }
  def printDebug(msg: String) {
    LogTools.mixDebug("[com.asiainfo.mix.streaming_log.LogAnalysisApp] DEBUG: " + msg)
  }
  def printWRANNING(msg: String) {
    LogTools.mixError("[com.asiainfo.mix.streaming_log.LogAnalysisApp] WRANNING: " + msg)
  }
  def printError(msg: String) {
    LogTools.mixError("[com.asiainfo.mix.streaming_log.LogAnalysisApp] ERROR: " + msg)
  }
}