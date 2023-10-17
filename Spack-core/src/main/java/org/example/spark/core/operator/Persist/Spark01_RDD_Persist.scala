package org.example.spark.core.operator.Persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)
    val list =List("Hello Scala","Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map((_, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println("***************************")


    val rdd1 = sc.makeRDD(list)

    val flatRDD1 = rdd1.flatMap(_.split(" "))

    val mapRDD1 = flatRDD1.map((_, 1))

    val groupRDD = mapRDD1.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop();
  }
}
