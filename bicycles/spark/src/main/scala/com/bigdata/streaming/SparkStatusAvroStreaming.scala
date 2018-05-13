package com.bigdata.streaming

import com.bigdata.hbase.StatusHBaseService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema.Parser
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{KafkaUtils, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}

object SparkStatusAvroStreaming {

  def main(args: Array[String]): Unit = {
    val topic = args(0)
    val brokers = args(1);
    val pause = args(2).toInt;

    val conf = new SparkConf().setAppName("SparkStreamingAvro ").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // streams will produce data every pause
    val ssc = new StreamingContext(sc, Seconds(pause))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers, //"localhost:9092,anotherhost:9092"
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "spark_status_streaming_group_id",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean),
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> (33000: java.lang.Integer), //todo:check this property
      "schema.registry.url" -> "http://sandbox-hdp.hortonworks.com:8081"
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Array(topic),
        kafkaParams)
    )

    stream.glom().foreachRDD(r => {
      r.foreach(arrayOfRecords => {
        println("*** got new records, size = " + arrayOfRecords.size)
        if (!arrayOfRecords.isEmpty) {
          val statuses = arrayOfRecords.map(rec => Status.parseFromJson(rec.value.asInstanceOf[GenericRecord].toString)).toList
          val hBaseService = new StatusHBaseService
          hBaseService.put(statuses)
        }
      })
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
