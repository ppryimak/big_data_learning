package com.bigdata

import java.util.concurrent.TimeUnit
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

object StatusProducer {

  val STATUS_URL = "https://gbfs.fordgobike.com/gbfs/es/station_status.json";
  val statusTransformer = new StatusTransformer

  def main(args: Array[String]): Unit = {

    val cycles = args(0).toInt //number of ievents
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

    for (nEvents <- Range(0, cycles)) {
      val now = new Date().getTime().toString
      val statusJson =
        getOriginalStatusJson(STATUS_URL);
      val events = statusTransformer.transform(statusJson)
      println("SENDING " + events.size);
      events.foreach(status=> {
        val event = statusTransformer.toJson(status).toString
        val data = new ProducerRecord[String, String](topic, status.stationId, event)
        producer.send(data)
      })

      //async
      //producer.send(data, (m,e) => {})
      //sync
      println("SENT");
      TimeUnit.SECONDS.sleep(pause);
    }

    println("sent per second: " + cycles * 1000 / (System.currentTimeMillis() - startMillis))
    producer.close()
  }

  def getOriginalStatusJson(url: String) = scala.io.Source.fromURL(url).mkString
}
