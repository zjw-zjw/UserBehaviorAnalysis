package com.zjw.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

/**
  *   Kafka生产者脚本
  */
object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {

    writeToKafkaWithTopic("hotitems")
  }


  def writeToKafkaWithTopic(topic: String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 创建一个 KafkaProducer，用来发送数据
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    // 从文件读取数据，逐条发送
    val bufferSource: BufferedSource = io.Source.fromFile("D:\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }
}
