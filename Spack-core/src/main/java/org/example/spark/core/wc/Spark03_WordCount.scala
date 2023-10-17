package org.example.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {



     val sparkConf =  new SparkConf().setMaster("local").setAppName("wordCount")
     val sc = new SparkContext(sparkConf)


    val lines:RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))



    val wordToOne = words.map(word => (word, 1))

    //Spark框架，可以直接進行分組與聚合
    //reduceByKey:相同的key的數據，可以對value進行reduce聚合
    //如果參數是按照順序的話又只執行一次可以使用 _ 來代替
    //    wordToOne.reduceByKey((x,y)=>{x+y})
    //    wordToOne.reduceByKey((x,y)=>x+y)
    var wordToCount = wordToOne.reduceByKey(_ + _);

    //5.將轉換結果採集到控制台輸出
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //TODO 關閉連接
    sc.stop()

  }
}
