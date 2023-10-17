package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //跟sql一樣左連結跟右連結

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2)), 2)

    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5),("c",6)), 2)

    val value = rdd.leftOuterJoin(rdd2)
    val value1 = rdd.rightOuterJoin(rdd2)
//
//    value.collect().foreach(println)
     value1.collect().foreach(println)


    sc.stop();
  }
}
