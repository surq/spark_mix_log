package com.asiainfo.spark.stream.report

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import java.io.FileInputStream
import java.io.BufferedInputStream
import java.io.File
import java.io.InputStream
import java.io.IOException

/**
 * @author surq
 * 上传ＨＤＦＳ文件
 */
object SaveHdfsFilesUtil {
  

  val conf = new Configuration()
//  conf.addResource(new Path("conf/core-site.xml"))
  val HDFSFileSytem = FileSystem.get(conf)
  
  /**
   * 移动本地文件到HDFS，成功复制本地文件到HDFS后删除本地文件
   * @return true. 如果成功返回true
   */
  def moveFileToHdfs(localFilePath: String, hdfsPath: String): Boolean = {
    
    //TODO
    val localFile = new File(localFilePath)
    if (localFile.exists && copyFileToHdfs(localFilePath, hdfsPath)) {
      try {
        localFile.delete()
      } catch {
        case ioe: Exception => ioe.printStackTrace(); true
      }
    } else false
  }

  /**
   * 判断此文件或目录在ＨＤＦＳ上是否存在
   */
  def isFileExists(path: String) = HDFSFileSytem.exists(new Path(path))
  def isDeleteFile(fileName: String) = new File(fileName).delete
  def mkdirs(path: String) = HDFSFileSytem.mkdirs(new Path(path))

  /**
   * 复制本地文件到HDFS
   * @return true. 如果成功返回true
   */
  def copyFileToHdfs(localPathStr: String, hdfsPath: String): Boolean = {

    if (!keepHDFSpath(hdfsPath)) false
    val in = new BufferedInputStream(new FileInputStream(localPathStr))
    val out = HDFSFileSytem.create(new Path(hdfsPath))
    try {
      IOUtils.copyBytes(in, out, 4096, false)
      true
    } catch {
      case e: Exception => e.printStackTrace(); false
    } finally {
      IOUtils.closeStream(in)
      IOUtils.closeStream(out)
    }
  }
 
  /**
   * 确保HDFS上有此路径
   * @return true. 如果成功返回true
   */
  def keepHDFSpath (hdfsPath:String): Boolean = {
    var Hpath = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"))
    if (Hpath.trim == "") Hpath = "/"
    try {
      if (!isFileExists(Hpath)) {
        println(Hpath + "[WARNNING:]  IN HDFS IS NOT EXISTED!")
        if (SaveHdfsFilesUtil.mkdirs(Hpath)) true else false
      }else true
    } catch {
      case ex: IOException => ex.printStackTrace(); false
      case e: Exception => e.printStackTrace();false
    }
  }
}