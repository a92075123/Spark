package org.example.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //reduce
    val i :Int = rdd.reduce(_ + _)
    //println(i)

    //collect:方法會將不同分區的數據按照分區順序採集到Driver端內存中，形成數據
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(" "))

    val l = rdd.count()
    println(l)

    val first = rdd.first()
    println(first)

    val ints1: Array[Int] = rdd.take(3)
    println(ints1.mkString(","))


    //takeOrdered:數據排序後，取3個數據
    val rdd2 = sc.makeRDD(List(4, 2, 3, 1))
    val ints2: Array[Int] = rdd.takeOrdered(3)
    println(ints2.mkString(","))

    sc.stop()
  }
}
