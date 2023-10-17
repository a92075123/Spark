package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-filter 符合規則數據保留
    val rdd = sc.makeRDD(List(1,2,3,4))

    val filter: RDD[Int] = rdd.filter(num => num % 2 != 0)

    filter.collect().foreach(println)






    sc.stop();
  }
}
