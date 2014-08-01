package com.asiainfo.mix.streaming_log

import kafka.producer.KeyedMessage
import java.sql.DriverManager
import java.sql.SQLException
import java.net.InetAddress
import java.net.Socket
import java.io.DataOutputStream
import java.io.IOException
import org.apache.spark.Logging
import scala.io.Source
import java.io.FileWriter
import java.util.Calendar
import java.io.File
import scala.beans.BeanProperty
import java.text.SimpleDateFormat
import java.text.DecimalFormat
import scala.collection.mutable.ArrayBuffer
import java.sql.SQLException


object LogTools extends Logging {

  /**
   * 创建日志写文件，写入器FileWriter
   */
  def mixLogWriter: FileWriter = {
    val userhome = System.getProperty("user.home")
    val nowTime = Calendar.getInstance().getTimeInMillis()
    val logdir = userhome + System.getProperty("file.separator") + "mix_spark_outlog/"
    val logfile = logdir + nowTime + ".log"
    val logfolder = new File(logdir)
    if (!logfolder.isDirectory()) logfolder.mkdirs()
    new FileWriter(logfile, true)
  }

  /**
   * 　返回log所在的时间段
   */
  def timeFlg(unix_time_str: String, split: String): String = {
    val dateFormat = new java.text.SimpleDateFormat("mm")
    val mmstr = dateFormat.format(unix_time_str.toLong);
    val flg = (mmstr.toInt) / (split.toInt)
    flg.toString
  }

  /**
   * 　把长整型的时间转成"年月日时分"
   */
  def timeConversion_H(unix_time_str: String): String = {
    var datatime = ""
    if (unix_time_str.length < 13) {
      datatime = unix_time_str + "0000000000000"
      datatime = datatime.substring(0, 13)
    } else datatime = unix_time_str
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMddHH")
    dateFormat.format(datatime.toLong);
  }

  /**
   * 　提取出完整域名
   */
  def getDomain(http_str: String): String = {
    var urldomain = ""
    var index = http_str.indexOf("://")
    if (index != -1) {
      urldomain = http_str.substring(index + 3)
      index = urldomain.indexOf("/")
      if (index != -1) {
        urldomain = urldomain.substring(0, index)
      }
    }
    urldomain
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
   * 更新mysql表<br>
   * @param tbname:mysql表名<br>
   * @param tbstructArray:表结构[字段：字段类型]<br>
   * @param dbSourceArray:数据库驱动<br>
   * @param selectKeyArray:查询字段键值对<br>
   * @param record:包函tbname的所有字段的键值对<br>
   */
  def updataMysql(tbname: String, dbSourceArray: Array[(String, String)], selectKeyArray: Array[(String, String)], record: Array[(String, String)]) {

    val dbSourceMap = dbSourceArray.toMap
    val recordMap = record.toMap
    Class.forName(dbSourceMap("driver"))
    val conn = DriverManager.getConnection(dbSourceMap("url"), dbSourceMap("user"), dbSourceMap("password"))

    try {
      val stmt = conn.createStatement()
      //查询条件 (主键部分若为空则当“0”处理)
      val whereExp = (for (f <- selectKeyArray) yield f._1 + "='" + (if(f._2.trim == "")"0" else f._2 )+ "'").mkString(" and ")
      // 要更新字段(除主key)
      val updataKeys = (for (f <- record if !selectKeyArray.contains(f)) yield f._1)
      val updatedColExp = updataKeys.mkString(",")

      val sqlstr = "select " + updatedColExp + " from " + tbname + " where " + whereExp
      mixInfo(LogTools.getClass().getName() + " INFO: 查询语句：" + sqlstr)
      val rs = stmt.executeQuery(sqlstr)

      // 判断查询结果有无数据
      var dbvalueMap = Map[String, String]()
      var updata_flag = true
      if (rs == null) {
        updata_flag = false
      } else {
        // 取mysql表中的记录
        while (rs.next()) {
          updataKeys.foreach(f => { dbvalueMap += (f -> (if (rs.getString(f) == null) "0" else rs.getString(f))) })
        }
        if (dbvalueMap.size > 0) updata_flag = true else updata_flag = false
      }
      var exec_sql_str = ""
      if (!updata_flag) {
        // insert
        val insertColsExp = (for (f <- record) yield f._1).mkString(",")
        // 如果主key字段为空时，变为"0"
        val insertvaluesExp = (for (f <- record) yield (if (f._2.trim=="")"0" else f._2)).mkString("'", "','", "'")
        exec_sql_str = "insert into " + tbname + "(" + insertColsExp + ") values (" + insertvaluesExp + ")"
      } else {

        //　去除主key部分
        val value = for { item <- record if (!selectKeyArray.contains(item)) } yield (item)
        value.foreach(f => { dbvalueMap += (f._1 -> (((dbvalueMap.getOrElse(f._1, "0")).toFloat + (f._2).toFloat).toString).replace(".0$", "")) })
        val dbvalueArray = dbvalueMap.toArray
        // update
        exec_sql_str = "update " + tbname + " set " + (for { item <- dbvalueArray } yield (item._1 + "='" + item._2 + "'")).mkString(" , ") + " where " + whereExp
      }
      mixInfo(LogTools.getClass().getName() + " INFO: 操作语句：" + exec_sql_str)
      stmt.executeUpdate(exec_sql_str)
    } catch {
      case sqlEx: SQLException => sqlEx.printStackTrace()
      case ex: Exception => ex.printStackTrace()
    } finally {
      conn.close()
    }
  }
  
  /**
   * @author马继
   * 守护进程
   */
  def heartBeat(topic: String) {
    try {
      val PORT = 4848
      val nowtime = System.currentTimeMillis() / 1000
      val ia = InetAddress.getByName("localhost")
      val socket = new Socket(ia, PORT)
      val out = new DataOutputStream(socket.getOutputStream())
      val hbstring = nowtime + ":this is from" + topic + "StreamingAPP"
      out.writeUTF(hbstring)
      out.flush()
      out.close()
      socket.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  /**
   * 把流数据变为mysql表中的结构（额外附加一个rowkey字段）<br>
   * @param tbItems:mysql表字段列表<br>
   * @param dbrecord:流计算的结果字段<br>
   * @param kafkaseparator:返回字符串时，字段拼接用到的连接符<br>
   *
   */
  def setTBSeq(rowkey: String, tbItems: Array[String], dbrecord: Map[String, String], kafkaseparator: String): String = {

    //变为db的字段的顺序
    val dbrecordArray = ArrayBuffer[(String, String)]()
    // 附加字段rowkey 后期merge用
    dbrecordArray += (("rowKey", rowkey))
    // db的字段加载
    tbItems.foreach(item => (dbrecordArray += ((item, dbrecord.getOrElse(item, "")))))
    // kafa send
    (for { item <- dbrecordArray } yield (item._2)).mkString(kafkaseparator)
  }

  /**
   * log 打印
   */
  def mixInfo(msg: String) = { log.info(msg) }
  def mixDebug(msg: String) = { if (true) log.debug(msg) }
  def mixError(msg: String) = { log.error(msg) }

  /**
   * 字符串分隔为定长的array<br>
   * @param textItem:要分隔的字符串<br>
   * @param separator:分隔符<br>
   * @param length:返回目标长度的数组<br>
   * 例：
   * str=“abc————”
   * 返回（"abc","","","",""）
   */
  def splitArray(textItem: String, separator: String, length: Int): Array[String] = {
    val srcarray = textItem.split(separator)
    val odeArray = ArrayBuffer[String]()
    odeArray ++= srcarray
    (srcarray.size until length).map(i => {
      odeArray += ""
    })
    odeArray.toArray
  }
}