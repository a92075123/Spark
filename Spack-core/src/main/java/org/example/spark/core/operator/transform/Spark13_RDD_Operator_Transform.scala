package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-sortBy 根據規則排序
    /*
      假設是用字串相比，會先比字串的第一個數字
      Ex:11<2
      在第2個參數加上false，本來默認是升序會變成降序
     */
    val rdd = sc.makeRDD(List(("1",1), ("11",2),("2",3)),2)

//    val newRDD = rdd.sortBy(t => t._1)

    val newRDD = rdd.sortBy(t => t._1.toInt,false)

    newRDD.collect().foreach(println)

    sc.stop();
  }
}
