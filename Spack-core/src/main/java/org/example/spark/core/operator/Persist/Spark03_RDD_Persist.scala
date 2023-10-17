package org.example.spark.core.operator.Persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)
    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("@@@@@@@@@@@@@@@@@@")
      (word, 1)
    })

    //spark原理，每一次新的操作(Ex:reduceBykey)，都會重頭去要數據，但是加上cache，會直接存在緩存裡，可以增加效率
    //就不會一直從頭拿數據，cache是把數據存到內存，persist是存到硬碟裡
    mapRDD.cache()
    mapRDD.persist()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println("***************************")
    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop();
  }
}
