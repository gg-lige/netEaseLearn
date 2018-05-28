package main.scala

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lg on 2018/5/28.
  *
  * SparkStreaming 学习
  * 从socket 端读取数据并统计词频
  */

// /opt/wwd/spark/bin/spark-submit --master spark://cluster1:7077 --executor-memory 5G --total-executor-cores 20 --driver-memory 6G --class main.scala.NetworkWordCount /opt/lg/NetEase/jar3/netEaseLearn.jar cluster1 9999


object NetworkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))

    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    // 对读入的数据进行分割、计数
    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
