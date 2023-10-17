package org.example.spark.core.operator.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark_RDD_Part {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd =sc.makeRDD(List(("nba","xxxxx"),
                            ("cba","xxxxx"),
                            ("wnba","xxxxx"),
                            ("nba","xxxxx"),
    ),3)
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")

    sc.stop()
  }
  /*
  自定義分區器
  1.繼承Partitioner
  2重寫方法
   */

  class MyPartitioner extends Partitioner {
    //分區的數量
    override def numPartitions: Int = 3

    //根據數據的key值返回數據的分區索引(從0開始)
    override def getPartition(key: Any): Int ={

      key match {
        case "nba" =>0
        case "wnba" =>1
        case _ =>2
      }


    }
  }
}
