package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-flatMap
    val rdd: RDD[String] = sc.makeRDD(
      List("Hello Scala","Hello Spark"
      ))
    val flatRDD: RDD[String] = rdd.flatMap(item => {
      item.split(" ")
    })

    flatRDD.collect().foreach(println)



    sc.stop();
  }
}
