package org.example.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {


    //Application
    //Spark框架
    //TODO 建立和Spark框架的連接
     val sparkConf =  new SparkConf().setMaster("local").setAppName("wordCount")
     val sc = new SparkContext(sparkConf)
    //TODO 執行業務邏輯

    val lines:RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))


    //幫每個字串都而外增加數字1的元素(hello,1)
    val wordToOne = words.map(word => (word, 1))

    //根據wordToOne的t._1進行分組，也就是針對同樣的詞分組，[(Hello,1), (Hello,1), (Hello,1),(Hello,1)]
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)


    //這裡是聚合操作(reduce)用於元素相加、求和、取最大值、取最小值等聚合操作
    //這裡的範例是t1._2的count=t2._2總共加了幾次，也就是這個list裡面有幾個相同元素就加幾次
    //例如(Hello,1), (Hello,1), (Hello,1), (Hello,1)t1._1詞不動，t1._2是總數的概念，t2._2因為有4個同單詞(Hello)所以會加四次1
    //所以t1._2=4，做完reduce都會返回(String, Int)，所以wordToCount是(String, Int)類型
    val wordToCount= wordGroup.map{
      case (word,list)=>{
        list.reduce((t1,t2)=>{
             (t1._1, t1._2 + t2._2)
        })
      }
    }

    //5.將轉換結果採集到控制台輸出
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //TODO 關閉連接
    sc.stop()

  }
}
