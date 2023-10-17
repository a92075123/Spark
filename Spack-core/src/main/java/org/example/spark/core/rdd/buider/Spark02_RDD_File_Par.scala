package org.example.spark.core.rdd.buider

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    //TODO 準備環境
    //local[*]=你有幾核心就用幾核
    //setAppName幫應用程序取名字
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 創建RDD
    /*
    textFile可以將文件作為數據處理的數據源，默認可以設定分區
    minPartitions:最小分區的數量
    math.min(defaultParallelism,2)
    如果不想默認分區數量，可用通過第二個參數指定分區數
    Spark讀取文件，底層其實使用的是Hadoop的讀取方式
    分區數量的計算
    totalSize=7 取決於文檔的字節數
    goalSize=7/2=3(byte) totalSize/設定的minPartitons = goalSize，3=每個分區放3個字節
    7/3=2...1，餘數每個分區的字節數的多少，如果他大於10%，要產生出新的分區，不然不會產生
    1/3=33%，33%大於10%所以會產生出新的分區
     */
    val rdd = sc.textFile("datas/1.txt",2)
    rdd.saveAsTextFile("output")




    //TODO 關閉環境
    sc.stop()
  }
}
