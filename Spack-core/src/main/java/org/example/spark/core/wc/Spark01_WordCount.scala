package org.example.spark.core.wc


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {


    //Application
    //Spark框架
    //TODO 建立和Spark框架的連接
     val sparkConf =  new SparkConf().setMaster("local").setAppName("wordCount")
     val sc = new SparkContext(sparkConf)
    //TODO 執行業務邏輯

    //1.讀取文件,獲取一行一行的數據
    // hello world
    val lines:RDD[String] = sc.textFile("datas")

    //2.將一行數據進行拆分，變成一個一個詞
    //扁平化:將整體拆分個體操作
    //"hello world" =>"hello" ,"world" "hello","world"
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3.將數據根據詞進行分組，再統計
    //(hello,hello,hello),(world,world)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //4.對分組後的數據進行轉換
    //(hello,hello,hello),(world,world)
    //(hello,3),(world,2)
    val wordToCount=wordGroup.map{
      case (word,list)=>{
        (word,list.size)
      }
    }
    //5.將轉換結果採集到控制台輸出
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //TODO 關閉連接
    sc.stop()

  }
}
