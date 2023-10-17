package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-flatMap
    //把陣列扁平化，也就是把數據從陣列全部取出來，在封裝成一個陣列裡面裝著都是數據，而不是陣列
    val rdd: RDD[List[Int]] = sc.makeRDD(
      List(List(1, 2), List(3, 4)
      ))
    val flatRDD: RDD[Int] = rdd.flatMap(
      list => {
        list
      }
    )
    flatRDD.collect().foreach(println)




    sc.stop();
  }
}
