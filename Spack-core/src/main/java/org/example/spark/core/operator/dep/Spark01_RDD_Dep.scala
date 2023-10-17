package org.example.spark.core.operator.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Dep{

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas/11.txt")
    println(lines.toDebugString)
    println("*******************")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    println(words.toDebugString)
    println("*******************")
    //幫每個字串都而外增加數字1的元素(hello,1)
    val wordToOne = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("*******************")

    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToCount.toDebugString)
    println("*******************")

    //5.將轉換結果採集到控制台輸出
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //TODO 關閉連接
    sc.stop()

  }
}
