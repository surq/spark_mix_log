package com.asiainfo.spark.stream.report

import java.util.Calendar
import java.net.InetAddress
import com.asiainfo.mix.streaming_log.KafkaProducer
import kafka.producer.KeyedMessage
import java.io.File
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import scala.beans.BeanProperty

/**
 * @author surq
 * @since 2014.08.12
 * @comment common<br>
 */
object LogReportUtil {
  
  /**
   * 获取localhost ip和机器名
   */
  def getLocalHostInfo: String = {
    // 获取本地ip信息
    val localhost = InetAddress.getLocalHost()
    val address = InetAddress.getLocalHost()
    val host = address.getHostName() + ":" + address.getHostAddress()
    host
  }

  /**
   * 获取系统时间
   */
  def getTime: String = {
    val datatime = Calendar.getInstance().getTime()
    val dateFormat = new java.text.SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    val mmstr = dateFormat.format(datatime);
    mmstr
  }

  /**
   * 获取系统时间
   */
  def getNowTime: String = {
    val datatime = Calendar.getInstance().getTime()
    val dateFormat = new java.text.SimpleDateFormat("YYYYMMddHHmmssSSS")
    val mmstr = dateFormat.format(datatime);
    mmstr
  }

  /**
   * evn:[kafka topic infomation],[topic name],[report separator]<br>
   * params:其它要输出信息<br>
   * 系统会自动在最后追加机器名和接收时间<br>
   * 向kafka　topic [report]上发送消息<br>
   * report 监控<br>
   */
  def monitor[T](iter: Iterator[T], env: Seq[String], params: Seq[String]): Iterator[T] = {
    val Seq(broker, reportTopic,topicName, separator) = env

    var res = ArrayBuffer[T]()
    iter.foreach(res += _)
    val count = res.size
    // heard: 依topic name开头,条数次之
    var msg = topicName + separator + count

    // body: 依次追加传入的参数
    params.foreach(f => {
      msg = msg + separator + f
    })
    //foot: 追加workNode's hostinfo和handletime
    val message = msg + separator + getLocalHostInfo + separator + getTime
    // kafa send
    kafkaSend(message, broker, reportTopic)
    res.iterator
  }

  /**
   * kafa 发送message
   */
  def kafkaSend(kafkaout: String, brokers: String, topic: String): Unit = {
    val producer = KafkaProducer.getProducer(brokers)
    var message = List[KeyedMessage[String, String]]()
    //KeyedMessage(topic hashkey message)
    message = new KeyedMessage[String, String](topic, kafkaout, kafkaout) :: message
    producer.send(message: _*)
  }

  /**
   * 修改文件名
   */
  def modifyFileName(fName: String): String = {
    val str = fName.substring(0, fName.indexOf("."))
    val newFileName = str + "_" + getNowTime + ".log"
    //    val renameFlg = new File(fName).renameTo(new File(newFileName))
    implicit def file(fName: String) = new File(fName)
    val renameFlg = fName renameTo newFileName
    if (renameFlg) newFileName else null
  }
}