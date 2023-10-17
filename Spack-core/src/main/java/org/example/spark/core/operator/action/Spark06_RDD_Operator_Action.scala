package org.example.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //foreach 是循環遍歷方法
    rdd.collect().foreach(println)
    println("*********")
    //foreach 不會按造順序打印
    rdd.foreach(println)

    sc.stop()
  }
}
