package org.example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

  def main(args: Array[String]): Unit = {

    //TODO 創建環境對象
    //StreamingContext，需要傳遞兩個參數
    //第一個是環境配置
    //第二個參數，批量處裡的處理時間，讀取數據的週期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf,Seconds(3));

    //TODO 邏輯處理
    //獲取端口數據
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(""))
    val wordToOne = words.map((_, 1))
    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    wordToCount.print()

    //啟動數據接收器
    ssc.start()
    //等待接收器關閉，程式就會關閉
    ssc.awaitTermination()


  }

}
