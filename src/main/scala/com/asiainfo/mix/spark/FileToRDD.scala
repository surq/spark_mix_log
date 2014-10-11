package com.asiainfo.mix.spark

import java.util.concurrent.LinkedBlockingQueue
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author 宿荣全
 * @since 2014.09.26
 * 把指定的文件转找成rdd并返回rddQueue
 */
class FileToRDD(rddQueue:LinkedBlockingQueue[(String,RDD[String])],sc: SparkContext,topicLogType:String,lbqHdfsFile: LinkedBlockingQueue[String]) extends Runnable {
  override def run = while (true) {
    val fileNmae = lbqHdfsFile.take()
    // _XXXX.tmp 此类型的文件为flume正在写入的文件，为不完正的HDFS文件
    if (!fileNmae.matches("""^_.*(\.tmp)$""")) rddQueue.offer((fileNmae,(new RddCreate).create(sc, topicLogType,fileNmae)))
  }
}