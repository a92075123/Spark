package org.example.spark.core.operator.Persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Persist {

  def main(args: Array[String]): Unit = {

    //cache:將數據臨時存在內存中
    //      會在連接父子關西中會添加新的依賴
    //persist:將數據臨時存在硬碟，因為硬碟IO，性能較低，但數據安全，如果任務執行完成，臨時保存數據文件就會丟失
    //checkpoint:將數據長久的保存在硬碟文件中，因為硬碟IO，性能較低，但數據安全，如果任務執行完成，臨時保存數據文件就會丟失
    //為了保證數據安全，一般情況下，會獨立執行操作，為了能提高效率，依般情況下，是需要和cache一起使用
    //執行過程中，會切斷連結，重新建立新的父與子連結關西，checkpoint等同於改變數據源


    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {

      (word, 1)
    })

    //mapRDD.cache()
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("***************************")
    println(mapRDD.toDebugString)



    sc.stop();
  }
}
