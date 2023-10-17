package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法
    val rdd = sc.textFile("datas/apache.log")

    //長的字詞串，轉換成短得字詞串
    val mapRDD = rdd.map(line => {
      val datas = line.split(" ")
      datas(6)
    })

    mapRDD.collect().foreach(println)

    sc.stop();
  }
}
