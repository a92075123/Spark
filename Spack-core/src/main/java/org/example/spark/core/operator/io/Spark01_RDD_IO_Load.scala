package org.example.spark.core.operator.io


import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Load {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("output1")
    println(rdd.collect().mkString(","))

    val rdd1=sc.objectFile[(String,Int)]("output2")
    println(rdd1.collect().mkString(","))


    sc.stop();
  }
}
