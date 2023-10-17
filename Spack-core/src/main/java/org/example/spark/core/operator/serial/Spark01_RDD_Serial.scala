package org.example.spark.core.operator.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")


    search.getMatch1(rdd).collect().foreach(println)

    sc.stop()
  }
  //查詢對象
  //類的構造參數是類的屬性
  class Search(query: String)extends Serializable {

    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    //函數序列化案例 需要再使用的類中繼承序列化
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    //屬性序列化案例，可以直接利用賦值就不用序列化了
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s =query
      rdd.filter(x => x.contains(s))
    }

  }


}
