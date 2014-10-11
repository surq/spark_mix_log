package com.asiainfo.mix.spark

import scala.xml.XML
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.beans.BeanProperty
/**
 * @author surq
 * @since 2014.09.19
 * ［配置文件XML解析］功能解介：<br>
 * 把conf/logbatchconf.xml解析返回解析结果值<br>
 */
object XmlProperiesAnalysis {


  def main(arge: Array[String]) {

	getXmlProperies
    val value =xmlProperiesAnalysis
    println("------------properiesMap:applacation---------------")
    value._1 foreach println
    println("-------------HDFSfilePathMap--------------")
    value._2 foreach println
    println("-------------dbSourceMap:db--------------")
    value._3 foreach println
    println("-------------logStructMap：log--------------")
    value._4 foreach println
    println("-------------tablesDefMap：mysql--------------")
    value._5 foreach println
  }
  
  var xmlProperiesAnalysis:Tuple5[Map[String, String], Map[String,Map[String,String]], Map[String, String], Map[String, Map[String, String]], Map[String, String]]  =_
  
  /**
   * @return:Tuple5
   * properiesMap:applacation properies 配置<br>
   * HDFSfilePathMap:输入日志文件类型以及路径(HDFS)配置<br>
   * dbSourceMap:db(mysql) 驱动配置<br>
   * logStructMap：log 日志属性配置<br>
   * tablesDefMap：mysql表定义配置<br>
   */
  def getXmlProperies: Tuple5[Map[String, String], Map[String,Map[String,String]], Map[String, String], Map[String, Map[String, String]], Map[String, String]] = {

    val xmlFile = XML.load("conf/logbatchconf.xml")

    //-----------------applacation  properies 配置-------------------------
    val properies = xmlFile \ "properies"
    val appName = (properies \ "appName").text.toString.trim
    val unionRDD_count = (properies \ "unionRDD_count").text.toString.trim
    val localLogDir = (properies \ "localLogDir").text.toString.trim
    val unionseparator = (properies \ "unionseparator").text.toString.trim
    val logSpace = (properies \ "logSpace").text.toString.trim()

    val properiesMap = Map[String, String]()
    properiesMap += ("appName" -> appName)
    properiesMap += ("unionRDD_count" -> unionRDD_count)
    properiesMap += ("localLogDir" -> localLogDir)
    properiesMap += ("unionseparator" -> unionseparator)
    properiesMap += ("logSpace" -> logSpace)

    //-----------------输入日志文件类型以及路径(HDFS)配置-----------------------
    val HDFSfilePathMap = Map[String,Map[String,String]]()
    val input = xmlFile \ "filePath" \ "input"
    input.foreach(f => {
      val topicLogType = (f \ "topicLogType").text.toString.trim
      val appClass = (f \ "appClass").text.toString.trim
      val loopInterval = (f \ "loopInterval").text.toString.trim
      val dir = (f \ "dir").text.toString.trim
      
      val hdfsProMap = Map[String,String]()
      hdfsProMap += ("topicLogType" -> topicLogType)
      hdfsProMap += ("appClass" -> appClass)
      hdfsProMap += ("loopInterval" -> loopInterval)
      hdfsProMap += ("dir" -> dir)
      HDFSfilePathMap += (topicLogType -> hdfsProMap)
    })

    //-----------------db(mysql) 驱动配置-----------------------------------
    val dbconf = xmlFile \ "dataSource" \ "dbSource"
    val driver = (dbconf \ "driver").text.toString.trim
    val url = (dbconf \ "url").text.toString.trim
    val user = (dbconf \ "user").text.toString
    val password = (dbconf \ "password").text.toString.trim

    val dbSourceMap = Map[String, String]()
    dbSourceMap += ("driver" -> driver)
    dbSourceMap += ("url" -> url)
    dbSourceMap += ("user" -> user)
    dbSourceMap += ("password" -> password)

    //-----------------log 日志属性配置--------------------------------------
    val logStructMap = Map[String, Map[String, String]]()

    val mixlogs = xmlFile \ "logProperties" \ "log"
    mixlogs.map(p => {
      val mixLogMap = Map[String, String]()
      val topicLogType = (p \ "topicLogType").text.toString.trim()
      val items = (p \ "items").text.toString.trim()
      val itemsDescribe = (p \ "itemsDescribe").text.toString.trim()
      val rowKey = (p \ "rowKey").text.toString.trim()
      val separator = (p \ "separator").text.toString.trim()

      mixLogMap += ("topicLogType" -> topicLogType)
      mixLogMap += ("items" -> items)
      mixLogMap += ("itemsDescribe" -> itemsDescribe)
      mixLogMap += ("rowKey" -> rowKey)
      mixLogMap += ("separator" -> separator)

      logStructMap += (topicLogType -> mixLogMap)
    })
    //-----------------mysql表定义配置---------------------------------------    
    val tables = xmlFile \ "tableDefines" \ "table"
    val tableName = (tables \ "tableName").text.toString.trim()
    val tb_items = (tables \ "items").text.toString.trim()
    val tb_itemsDescribe = (tables \ "itemsDescribe").text.toString.trim()
    val tb_describe = (tables \ "describe").text.toString.trim()
    val tablesDefMap = Map[String, String]()

    tablesDefMap += ("tableName" -> tableName)
    tablesDefMap += ("items" -> tb_items)
    tablesDefMap += ("itemsDescribe" -> tb_itemsDescribe)
    tablesDefMap += ("describe" -> tb_describe)

    // 返回解析结果
    xmlProperiesAnalysis = Tuple5(properiesMap, HDFSfilePathMap, dbSourceMap, logStructMap, tablesDefMap)
    xmlProperiesAnalysis
  }
}