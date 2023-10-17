package org.example.spark.core.operator.Persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Persist {

  def main(args: Array[String]): Unit = {

    //cache:將數據臨時存在內存中
    //persist:將數據臨時存在硬碟，因為硬碟IO，性能較低，但數據安全，如果任務執行完成，臨時保存數據文件就會丟失
    //checkpoint:將數據長久的保存在硬碟文件中，因為硬碟IO，性能較低，但數據安全，如果任務執行完成，臨時保存數據文件就會丟失
    //為了保證數據安全，一般情況下，會獨立執行操作，為了能提高效率，依般情況下，是需要和cache一起使用



    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("@@@@@@@@@@@@@@@@@@")
      (word, 1)
    })

    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println("***************************")
    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop();
  }
}
