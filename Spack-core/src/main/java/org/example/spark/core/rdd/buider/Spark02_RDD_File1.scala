package org.example.spark.core.rdd.buider

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    //TODO 準備環境
    //local[*]=你有幾核心就用幾核
    //setAppName幫應用程序取名字
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 創建RDD
    /*
     textFile:以行為單位來讀取數據，讀取的數據都是字符串
     wholeTextFiles:以文件為單位來讀取數據，讀取的結果表示為元組，第一個元素表示文件路徑，第二個元素表示文件內容
     */
    val rdd = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)

    //TODO 關閉環境
    sc.stop()
  }
}
