package org.example.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    wordCount9(sc)

    sc.stop()

    //group
    def wordCount1(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
      val value: RDD[(String, Int)] = group.mapValues(item => item.size)
    }

    //groupBykey
    def wordCount2(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordOne: RDD[(String, Int)] = words.map((_, 1))
      val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
      val value: RDD[(String, Int)] = group.mapValues(item => item.size)
    }

    //reduceBykey 效率較高
    def wordCount3(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordOne: RDD[(String, Int)] = words.map((_, 1))
      val value = wordOne.reduceByKey(_ + _)
    }

    //aggregateByKey
    def wordCount4(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordOne: RDD[(String, Int)] = words.map((_, 1))
      wordOne.aggregateByKey(0)(_ + _, _ + _)
    }

    //foldByKey 分區間跟分區內的
    def wordCount5(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordOne: RDD[(String, Int)] = words.map((_, 1))
      wordOne.foldByKey(0)(_ + _)
    }

    //foldByKey 分區間跟分區內的
    def wordCount6(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordOne: RDD[(String, Int)] = words.map((_, 1))
      wordOne.foldByKey(0)(_ + _)
    }

    //combineByKey 分區間跟分區內的
    def wordCount7(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordOne = words.map((_, 1))

      val value: RDD[(String, Int)] = wordOne.combineByKey(
        v => v,
        (x: Int, y) => x + y,
        (x: Int, y: Int) => x + y
      )
    }

    def wordCount8(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))
      val wordOne = words.map((_, 1))

      val stringToLong: collection.Map[String, Long] = wordOne.countByKey()
    }

    def wordCount9(sc: SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark","Hello World"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))

      val mapWord = words.map(word => {
        mutable.Map[String, Long]((word, 1))
      })
      val stringToLong = mapWord.reduce((map1, map2) => {
        map2.foreach {
          case (word, count) => {
            //如果map1跟map2的word有對應到的，會把word的value，也就是出現累積次數拿出來跟count相加
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      })
      println(stringToLong)
    }
  }
}
