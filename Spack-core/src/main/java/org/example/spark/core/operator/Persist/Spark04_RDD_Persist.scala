package org.example.spark.core.operator.Persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {

  def main(args: Array[String]): Unit = {

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

    //checkpoint需要指定檢查點保存路徑
    //檢查點路徑保存的文件，當作業執行完畢後，不會背刪除
    //一般保存路徑是在分布式存儲系統:HDFS
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println("***************************")
    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop();
  }
}
