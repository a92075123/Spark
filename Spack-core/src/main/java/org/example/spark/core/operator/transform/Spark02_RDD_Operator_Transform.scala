package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-Partitions
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //mapPartitions:可以以分區為單位進行數據轉換操作
    //              但是會將整個分區的數據儲存到內存進行操作
    //              如果處理完的數據是不會釋放掉的，要等到整個分區都處理完才會釋放掉
    //              在內存小，數據量大的時候，容易出現內存爆滿
    val mpRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        //依照分區走幾次，4/2，所以一個分區負責2個數字
        println(">>>>>>>>>>")
        iter.map(_ * 2)
      }
    )
    mpRDD.collect().foreach(println)

    sc.stop();
  }
}
