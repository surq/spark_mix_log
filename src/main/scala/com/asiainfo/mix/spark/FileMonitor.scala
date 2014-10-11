package com.asiainfo.mix.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.FileUtil
import java.io.FileNotFoundException
import java.util.concurrent.LinkedBlockingQueue
import java.io.FileWriter
import java.util.Calendar
import java.io.File
import java.io.IOException

/**
 * @author 宿荣全
 * @since 2014.09.22
 * ［HDFS粗粒度文件监控］功能解介：<br>
 * 1、对指定的ＨＤＦＳ路径以及该路径下的子文件夹作文件变更监控<br>
 * @param historyrddMap:File转换为rdd的历史记录拧成的hash 树；用于启动后从扫描路径中支除以前处理过的文件<br>
 * @param topicLogType:日志类型，用于创建日志类型的文件夹，内部生成日志：写入新扫描的增加文件名一览表<br>
 * @param locallogpath:存放日志的根目录<br>
 * @param hdfsPaht:指定的ＨＤＦＳ的路径(要扫描的HDFS根路径)<br>
 * @param scanInterval:扫描时间间隔<br>
 * @param lbqHdfsFile:新增加的文件全部押在此队列中<br>
 * 注：此监控就文件级的，只对文件增减做监控，不对文件内容修改做监控<br>
 */
class FileMonitor(historyrddMap:Map[String, ArrayBuffer[String]],topicLogType: String, locallogpath: String, hdfsPaht: String, scanInterval: Long, lbqHdfsFile: LinkedBlockingQueue[String]) extends Runnable with MixLog {

  val conf = new Configuration()
  //local test 用
//  conf.addResource(new Path("conf/core-site.xml"))
  val HDFSFileSytem = FileSystem.get(conf)

  // 内存中维持的一个文件，文件夹关联的map
  //[folderpath_Name,(modify_time,fileList_hashcode,fileList)]
  val baseFolderMap = Map[String, (Long, Int, ArrayBuffer[String])]()
  //最新扫出的有变化的文件夹想关信息［只记录新增的文件夹和已有文件夹下文件的新增文件信息］
  val scanModifyFolderMap = Map[String, ArrayBuffer[String]]()
  var hascodesMap = baseFolderMap.hashCode
  // 记录新增文件列表
  var logwriter: FileWriter = null
  override def run() {
    // 创建log 生成器，记录新增文件
    logwriter = Util.mixLogWriter(locallogpath, topicLogType)
    if (logwriter == null) return
    val hpath = new Path(hdfsPaht)
    if (!keepHDFSpath(hpath)) System.exit(0)
    // 初始化加载baseFolderMap和scanModifyFolderMap
    initLoad(hpath)
    while (true) {
      folderMonitor(hpath)
      fileNamePrint
      Thread.sleep(scanInterval)
      scanModifyFolderMap.clear
    }
  }

  /**
   * 打印结合项的值，供调试时使用
   */
  def fileNamePrint {
    if (hascodesMap != baseFolderMap.hashCode) {
      hascodesMap = baseFolderMap.hashCode
      //        println("-------------baseFolderMap-----------------------------")
      //        baseFolderMap.foreach(f => {
      //          val key = f._1
      //          val value = f._2
      //          println("folder:" + key)
      //          value._3 foreach println
      //        })
      //        println("-------------scanModifyFolderMap-----------------------")
      scanModifyFolderMap.foreach(f => {
        val key = f._1
        val value = f._2
        value.foreach(f => {
          logwriter.write(("新增文件：" +Util.getNowTime+"	" + f + System.getProperty("line.separator")))
          logwriter.flush
        })
      })
    }
  }

  /**
   * 维护基准表［baseFolderMap］和打描结果表［scanModifyFolderMap］<br>
   * 1、gcBaseFolderMap：删除当前ＨＤＦＳ已经不存在的文件夹<br>
   * 2、维护现在的在ＨＤＦＳ上也切实存在的文件夹<br>
   */
  def folderMonitor(hdfsPath: Path) {
    //删除已经移除的文件夹
    gcBaseFolderMap
    baseFolderMap.foreach(f => {
      try {
        val path = new Path(f._1)
        val scanfolderModifyTime = HDFSFileSytem.getFileStatus(path).getModificationTime()
        if (f._2._1 != scanfolderModifyTime) {
          fileScanner(path)
        }
      } catch {
        case ex: FileNotFoundException => ex.printStackTrace()
        case e: Exception => e.printStackTrace()
      }
    })
  }

  /**
   * 从内存中回收在ＨＤＦＳ中已不存在的那些路径
   */
  def gcBaseFolderMap {
    val baseArray = baseFolderMap.toArray
    val gcList = for (index <- 0 until baseArray.size if (!HDFSFileSytem.exists(new Path(baseArray(index)._1)))) yield (baseArray(index)._1)
    gcList.foreach(baseFolderMap.remove(_))
  }

  /**
   * baseFolderMap:总是保持指定目录下最新的新增文件结构<br>
   * scanModifyFolderMap:保存变更的文件部分<br>
   * 1、扫出新文件夹：将新建文件夹以及子文件夹，更新到基准表［baseFolderMap］和打描结果表［scanModifyFolderMap］<br>
   * 2、扫出新文件：置换baseFolderMap对应的key（文件夹名）的value，并把新增的文件追加到打描结果表［scanModifyFolderMap］<br>
   */
  def fileScanner(hdfsPath: Path) {
    val baseFolderInfo = baseFolderMap.getOrElse(hdfsPath.toString(), null)
    val scanfolderModifyTime = HDFSFileSytem.getFileStatus(hdfsPath).getModificationTime()
    // baseFolderInfo == null 说明是新增文件夹；新扫描的文件夹最后修改时间，跟对照表［baseFolderMap］中的修改时间不一致说明本目录中有文件增减

    val fileList = ArrayBuffer[String]()
    val status = HDFSFileSytem.listStatus(hdfsPath);
    status.foreach(f => {
      if (f.isDir) {
        // baseFolderInfo == null 说明是新增文件夹；
        if (baseFolderMap.getOrElse(f.getPath().toString(), null) == null) fileScanner(f.getPath())
      } else fileList += f.getPath.toString
    })

    if (baseFolderInfo == null) {
      // 把新增文件压入队列
      putQueue(fileList)
      // 更新扫描变更结果文件：scanModifyFolderMap
      scanModifyFolderMap += (hdfsPath.toString -> fileList)
      //重置此path下变更后的内容
      baseFolderMap += (hdfsPath.toString -> (scanfolderModifyTime, fileList.mkString.hashCode, fileList))
    } else {
      // 目录有文件增减
      // 文件列表hashcode有变化说明文件有增减
      if (baseFolderInfo._2 != fileList.mkString.hashCode) {
        // 文件列表的hashcode不一致说明是新增减了文件，相等说明是子目录的增加或子目录文件增加导致
        val basetArray = baseFolderInfo._3
        // 文件夹内有加文件操作
        val addindex = for (index <- 0 until fileList.size if (!basetArray.contains(fileList(index)))) yield (index)
        // 选出新增文件列表
        val addfileList = ArrayBuffer[String]()
        addindex.foreach(addfileList += fileList(_))
        // 把新增文件压入队列
        putQueue(addfileList)
        // 更新扫描变更结果文件：scanModifyFolderMap
        scanModifyFolderMap += (hdfsPath.toString -> addfileList)
        //重置此path下变更后的内容
        baseFolderMap += (hdfsPath.toString -> (scanfolderModifyTime, addfileList.mkString.hashCode, fileList))
      }
    }
  }

  /**
   * 把新增文件压入队列
   */
  def putQueue(list: ArrayBuffer[String]) {
    list.foreach(lbqHdfsFile.offer(_))
  }

  /**
   * 初始化加载，指定路径下完全扫描<br>
   * 扫描指定路径下的文件，以及子文件返回各文件夹的路径名称、最后修改时间、和文件列表。<br>
   * 装载基准表［baseFolderMap］和打描结果表［scanModifyFolderMap］<br>
   */
  def initLoad(hdfsPath: Path) {
    val folderInfo = HDFSFileSytem.getFileStatus(hdfsPath)
    val modifyTime = folderInfo.getModificationTime
    val fileList = ArrayBuffer[String]()
    val status = HDFSFileSytem.listStatus(hdfsPath);
    status.foreach(f => {
      if (f.isDir) initLoad(f.getPath()) else {
        // 已有历史记录，非初次处理(重启操作)
        if (historyrddMap.size > 0) {
          var hdpath =hdfsPath.toString
          if (!hdpath.startsWith("hdfs://")) {
            // 拼接HDFS文件系统名称［hdfs://localhost:port］
            hdpath = (HDFSFileSytem.getUri()).toString + hdpath
          }
          // 历史记录中没有处理过此条
          if (historyrddMap.getOrElse(hdpath, null) == null) {
            // 整个文件夹都没有
            fileList += f.getPath.toString
          } else {
            val list = historyrddMap(hdpath)
            // 此文件夹下已有数据被处理过，但此文件未被处理
            if (!list.contains(f.getPath.toString)) fileList += f.getPath.toString
          }
        } else {
          // 初次启动
          fileList += f.getPath.toString
        }
      }
    })
    val hashcode = fileList.mkString.hashCode()
    baseFolderMap += (hdfsPath.toString -> (modifyTime, fileList.mkString.hashCode, fileList))
    // 把新增文件压入队列
    putQueue(fileList)
    scanModifyFolderMap += (hdfsPath.toString -> fileList)
  }

  /**
   * 确保HDFS上有此路径
   * @return true. 如果成功返回true
   */
  def keepHDFSpath(hdfsPath: Path): Boolean = {
    if (!HDFSFileSytem.exists(hdfsPath)) {
      println(hdfsPath + "[WARNNING:]  IN HDFS IS NOT EXISTED!")
      false
    } else {
      println(hdfsPath + "[INFO:]  IS LOADING!")
      true
    }
  }

}