package com.asiainfo.spark.stream.report

import java.util.concurrent.LinkedBlockingQueue

/**
 * @author surq
 * 上传ＨＤＦＳ文件，删除本地文件
 */
class SaveHdfsFile(args: Array[String], lbqHdfsFile: LinkedBlockingQueue[String]) extends Runnable {
  val Array(reporttopic, fileLineSize, logpath, hdfsPaht) = args
  var retransfer_flg = false
  val lbqHdfsFileFail = new LinkedBlockingQueue[String]()

  override def run() {
    println("------SaveHdfsFile ------")
    while (true) {
      val localFilePath = lbqHdfsFile.take()
      val hdfsFileName = hdfsPaht + localFilePath.substring(localFilePath.lastIndexOf("/"))
      val transfer_flg = SaveHdfsFilesUtil.moveFileToHdfs(localFilePath, hdfsFileName)
      // 传输失败
      if (!transfer_flg) {
        // 本地文件删除失败情况
        if (SaveHdfsFilesUtil.isFileExists(hdfsFileName)) {
          SaveHdfsFilesUtil.isDeleteFile(localFilePath)
        } else {
          // 开启再向ＨＤＦＳ传输的线程
          if (!retransfer_flg) {
            retransfer_flg = true
            //开启再向ＨＤＦＳ传输的线程
            new Thread(new Runnable {
              override def run() {
                while (true) {
                  val fail_localFileName = lbqHdfsFileFail.take()
                  val fail_hdfsFileName = hdfsPaht + fail_localFileName.substring(fail_localFileName.lastIndexOf("/"))
                  val retransfer_flg = SaveHdfsFilesUtil.moveFileToHdfs(fail_localFileName, fail_hdfsFileName)
                  if (!retransfer_flg) {
                    if (SaveHdfsFilesUtil.isFileExists(fail_hdfsFileName)) {
                      SaveHdfsFilesUtil.isDeleteFile(fail_localFileName)
                    } else {
                      lbqHdfsFileFail.offer(fail_localFileName)
                      Thread.sleep(2000)
                    }
                  } else {
                    println("[INFO:] succeed transfer to HDFS-FILESYSTEM:" + hdfsFileName)
                  }
                }
              }
            }).start()
          }
          lbqHdfsFileFail.offer(localFilePath)
        }
      } else {
        println("[INFO:] succeed transfer to HDFS-FILESYSTEM:" + hdfsFileName)
      }
    }
  }
}
