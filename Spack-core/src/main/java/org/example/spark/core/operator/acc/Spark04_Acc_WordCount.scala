package org.example.spark.core.operator.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello","spark","hello"))

      //累加器:WordCount
      //創建累加器
      val wcAcc = new MyAccumulator()
      //向Spark進行註冊
      sc.register(wcAcc,"wordCountAcc")

    rdd.foreach(
      word=>{
        //數據的累加(使用累加器)
        wcAcc.add(word)
      }
    )

    //獲取累加器的結果
    println(wcAcc.value)



    sc.stop()
  }
  /*
  自訂義累加器
  1.繼承AccumulatorV2，定義泛型
  IN:累加器輸入的數據類型 String
  OUT:累加器返回的數據的類型 mutable.Map[String,Long]

  2.重寫方法



   */
  class  MyAccumulator extends  AccumulatorV2[String,mutable.Map[String,Long]]{

    private var wcMap =mutable.Map[String,Long]()

    //判斷是否為初始狀態
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit ={
      wcMap.clear()
    }

    //獲取累加器，需要計算的值
    override def add(v: String): Unit ={
     val newCnt =  wcMap.getOrElse(v,0L) + 1
     wcMap.update(v,newCnt)
    }


    //合併多個累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      //當前map
      val map1 = this.wcMap
      //下一個map
      val map2 =other.value

      map2.foreach {
        case (word, count) => {
            val newCount = map1.getOrElse(word,0L)+count
          map1.update(word,newCount)
        }
      }
    }


    //累加器結果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
