package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-coalesce 縮減分區
    val rdd = sc.makeRDD(List(1, 2, 3, 4,5,6),3)

    //coalesce方法默認是不會將分區數據打亂再重新組合
    //這種情況會導致分區數量不均衡，導致數據傾斜
    //如果想要讓數據平均，可以在第2個參數加上true，但是數據會被打亂，所以會出現數據是隨機分配的
    val newRDD: RDD[Int] = rdd.coalesce(2,true)

    newRDD.saveAsTextFile("output")

    sc.stop();
  }
}
