package org.example.spark.core.rdd.buider

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    //TODO 準備環境
    //local[*]=你有幾核心就用幾核
    //setAppName幫應用程序取名字
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 創建RDD
    /*
    1234567@@=>012345678
    89@@=>9101112
    0=>13
    [0,7] =>1234567
    [7,14]=>9101112
    如果數據元為多個文件，那麼計算分區以文件為單位進行分區
     */
    val rdd = sc.textFile("datas/word.txt")
    rdd.saveAsTextFile("output")




    //TODO 關閉環境
    sc.stop()
  }
}
