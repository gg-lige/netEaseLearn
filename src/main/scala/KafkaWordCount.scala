package main.scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by lg on 2018/5/28.
  *
  * jar5
  */


// /opt/wwd/spark/bin/spark-submit --master spark://cluster1:7077 --executor-memory 5G --total-executor-cores 20 --driver-memory 6G --class main.scala.KafkaWordCount /opt/lg/NetEase/jar5/netEaseLearn.jar cluster1:2181 test-consumer-group test 1

object KafkaWordCount {

  def main(args: Array[String]) = {
    //对输入参数进行验证，若少于4个退出
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum><group><topics><numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args

    //初始话spark Streaming 环境
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("checkpoint")

    //通过zookeeper连接属性，获取Kafka的组和主题信息，并创建连接获取数据流
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    //对获取的数据流进行分词，使用reduceByKeyAndWindow进行统计并打印
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
