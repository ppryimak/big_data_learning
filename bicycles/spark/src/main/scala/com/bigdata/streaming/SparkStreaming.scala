package com.bigdata.streaming

import java.util

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{KafkaUtils, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val topic = args(0)
    val brokers = args(1);
    val pause = args(2).toInt;

    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(pause))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers, //"localhost:9092,anotherhost:9092"
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "spark_status_streaming_group_id",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean),
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> (33000: java.lang.Integer) //todo:check this property
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Array(topic),
        kafkaParams)
    )

    stream.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      r.foreach(s => {
        println("Key is " + s.key)
        val statuses = Status.parseFromJson(s.value);
        println(statuses)
      }
      )
    })

    ssc.start()

    println("*** started termination monitor")

    // streams seem to need some time to get going
    Thread.sleep(5000)

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread", e)
      }
    }

    // stop Spark
    sc.stop()

    println("*** done")
  }
}
