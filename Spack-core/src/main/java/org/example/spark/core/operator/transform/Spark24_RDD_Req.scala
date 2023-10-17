package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //案例操作:統計出每一個省份的每一個廣告被點擊的數量排行TOP3

    //1.獲得原始數據
    //時間，省分，城市，用戶，廣告

    val dataRDD = sc.textFile("datas/agent.log")

    //2.將原始數據進行轉換，方便統計，轉換成((省分,廣告),1)
    val value: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )

    //3.將轉換的結構數據，進行分組聚合
    //((省分,廣告),1)=>((省分,廣告),sum)
    val value1: RDD[((String, String), Int)] = value.reduceByKey(_ + _)

    //4.將聚合的結果進行結構的轉換
    //((省分,廣告),sum)=>(省份,(廣告,sum))
    val value2 = value1.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }

    //5.將轉換結構的數據進行分組
    //(省份,(廣告A,sumA)),(省份,(廣告B,sumB))
    val value3: RDD[(String, Iterable[(String, Int)])] = value2.groupByKey()


    //6.將分組後的數據進行排序(根據數量排序，取最高的前三名)
    val value4 = value3.mapValues(
      iter => {
        //Ordering.Int.reverse(降序)
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    //7.輸出到控制台
    value4.collect().foreach(println)

    sc.stop();
  }
}
