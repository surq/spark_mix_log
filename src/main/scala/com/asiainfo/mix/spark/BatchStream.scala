package com.asiainfo.mix.spark

import com.asiainfo.mix.streaming_log.LogTools
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map

/**
 * @author 宿荣全
 * @since 2014.09.26
 * 所有log处理都要实现此类<br>
 */
abstract class BatchStream {
  def run(topicLogType: String,inputStream:RDD[Array[(String, String)]]): RDD[String]
}