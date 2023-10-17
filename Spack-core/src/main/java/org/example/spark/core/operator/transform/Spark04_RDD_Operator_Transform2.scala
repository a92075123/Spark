package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-flatMap
    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))

    /*
    這裡的 flatMap 轉換對 rdd 中的每個元素進行操作。每個元素都會進入 data => {...} 這個函式。
    接著，使用了模式匹配 (match) 來檢查 data 的類型：
    如果 data 是一個 List (case list: List[_] => list):
    直接返回該列表。這意味著，如果 RDD 中的一個元素是一個包含三個整數的列表，那麼 flatMap 會返回這三個整數作為三個單獨的元素。
    如果 data 不是一個 List (case dat => List(dat)):
    將 data 放入一個新的列表中並返回。這意味著，如果 RDD 中的一個元素是一個單獨的整數，那麼 flatMap 會返回一個包含該整數的列表。
    結果是，原始的 rdd 被轉換成一個新的 RDD flatRDD，其中所有的列表都被展平，而單獨的整數則保持不變。
    例如，考慮原始 RDD 的內容：List(List(1,2),3,List(4,5))。使用上述的 flatMap 轉換後，flatRDD 的內容將是：List(1, 2, 3, 4, 5)。
     */
    val flatRDD = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )
    flatRDD.collect().foreach(println)



    sc.stop();
  }
}
