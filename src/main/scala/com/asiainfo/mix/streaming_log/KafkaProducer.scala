package com.asiainfo.mix.streaming_log

import kafka.producer.{ProducerConfig, Producer}
import java.util.Properties
import scala.collection.mutable

object KafkaProducer {

  val producermap = new mutable.HashMap[String,Producer[String,String]]()

  def getProducer(brokers:String):Producer[String,String]={
    val producer = producermap.getOrElse(brokers,{
      val props = getProducerConfig(brokers)
      val pro = new Producer[String, String](new ProducerConfig(props))
      producermap.put(brokers,pro)
      pro
    })
    producer
  }

  def getProducerConfig(brokers: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props
  }
}