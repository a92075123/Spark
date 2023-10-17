package org.example.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val rdd = sc.makeRDD(List(("a",1),("a",2), ("a",3)))

    //countByValue統計出現同樣的value的數量
    //val intToLong: collection.Map[Int, Long] = rdd.countByValue()

    //countByKey統計出現同樣的key的數量
    rdd.countByKey().foreach(println)



    sc.stop()
  }
}
