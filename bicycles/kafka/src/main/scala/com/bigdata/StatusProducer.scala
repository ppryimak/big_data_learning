package com.bigdata

import java.util.concurrent.TimeUnit
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

object StatusProducer {

  val STATUS_URL = "https://gbfs.fordgobike.com/gbfs/es/station_status.json";

  def main(args: Array[String]): Unit = {

    val events = args(0).toInt //number of ievents
    val topic = args(1) //topic
    val brokers = args(2) //"sandbox-hdp.hortonworks.com:6667"
    val pause = args(3).toLong //fake pause between evetns
    val rnd = new Random()
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "StatusProducer")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val startMillis = System.currentTimeMillis()

    for (nEvents <- Range(0, events)) {
      val now = new Date().getTime().toString
      val statusJson =
        getOriginalStatusJson(STATUS_URL);
      println("SENDING " + now);
      val data = new ProducerRecord[String, String](topic, now, statusJson)

      //async
      //producer.send(data, (m,e) => {})
      //sync
      producer.send(data)
      TimeUnit.SECONDS.sleep(pause);
    }

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - startMillis))
    producer.close()
  }

  def getOriginalStatusJson(url: String) = scala.io.Source.fromURL(url).mkString
}
