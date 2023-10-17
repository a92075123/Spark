package org.example.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)))

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    //要求數據類型是K-V
    rdd.saveAsSequenceFile("output2")


    sc.stop()
  }
}
