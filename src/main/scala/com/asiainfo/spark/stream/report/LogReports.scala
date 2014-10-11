package com.asiainfo.spark.stream.report
import org.apache.spark.streaming.StreamingContext._
import java.util.Properties
import scala.xml._
import java.util.Date
import kafka.consumer._
import java.text.SimpleDateFormat
import sys.process._
import java.util.concurrent.LinkedBlockingQueue
import org.apache.hadoop.conf.Configuration

/**
 * @author surq
 * @since 2014.08.12
 * @comment 监控spark stream接收数据情况<br>
 * @param args(监控topic，每个HDFS文件的记录数，监控log生成的本地路径，上传hdfs的路径)
 */
object LogReports {
  
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("Usage: <topic fileLineSize Localpath HDFSpath>")
      return
    }
    val Array(topic, cycle, path,hdfsPaht) = args
    
    val lbq = new LinkedBlockingQueue[String]()
    val lbqHdfsFile = new LinkedBlockingQueue[String]()
    new Thread(new LogReportsAnalysis(args, lbq,lbqHdfsFile)).start()
    new Thread(new SaveHdfsFile(args, lbqHdfsFile)).start()
     
    // kafa 接收数据
    val consumer = Consumer.create(createConsumerConfig)
    val msgList = consumer.createMessageStreams(Map(topic -> 1)).get(topic).get(0)
    for (messageAndTopic <- msgList) {
      val message = new String(messageAndTopic.message)
      lbq.offer(message)
    }
  }

  def createConsumerConfig: ConsumerConfig = {
    val props = new Properties()

    props.put("zookeeper.connect", "localhost:2187");
    props.put("group.id", "test-consumer-group")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")

    val consumerConfig = new ConsumerConfig(props)
    consumerConfig
  }
}