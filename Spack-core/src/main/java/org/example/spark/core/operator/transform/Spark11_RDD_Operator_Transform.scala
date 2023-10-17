package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-repartition&&coalesce 增加分區
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //coalesce可以增加分區，但是第2個參數要是true，進行shuffle操作，要打亂數據重新組合才有用
    //val newRDD: RDD[Int] = rdd.coalesce(3,true)

    //方法2 會直接啟用shuffle
    val newRDD: RDD[Int] =rdd.repartition(3)

    newRDD.saveAsTextFile("output")

    sc.stop();
  }
}
