package main.scala
import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Created by lg on 2018/5/28.
  *
  * jar4
  */

//  java -cp /opt/lg/NetEase/jar4/netEaseLearn.jar main.scala.KafkaWordCountProducer cluster1:9092 test 3 5

object KafkaWordCountProducer {

  def main(args: Array[String]) = {
    //对输入参数进行验证，若少于4个退出
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metedataBrokerList><topic><messagesPerSec><wordsPerMessage>")
      System.exit(1)
    }
   val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    //Zookeeper 连接属性，通过第一个传入参数获取brokers地址信息
    val props = new util.HashMap[String,Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    //创建Kafka示例，该实例通过参数确定每秒发送消息的条数，及每个消息包含0-9随机数的个数
    val producer = new KafkaProducer[String,String](props)
    while(true){
      (1 to messagesPerSec.toInt).foreach{messageNum=>
        val str = (1 to wordsPerMessage.toInt).map(x=> new Random().nextInt(10).toString).mkString(" ")
        val message = new ProducerRecord[String,String](topic,null,str)

        //调用Kafka发送消息
        producer.send(message)

      }
      Thread.sleep(1000)
    }

  }
}
