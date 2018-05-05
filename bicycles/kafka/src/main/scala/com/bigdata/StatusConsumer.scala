package com.bigdata

import java.util.concurrent._
import java.util.{Collections, Properties}

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._

object StatusConsumer extends Logging {
  def main(args: Array[String]): Unit = {
    val brokers = args(0)
    val groupId = args(1)
    val topic = args(2)
    execute(brokers, groupId, topic);
  }

  def execute(brokers:String, groupId:String, topic:String) = {

    def createConsumerConfig(brokers: String, groupId: String): Properties = {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props
    }

    val props = createConsumerConfig(brokers, groupId)
    val consumer = new KafkaConsumer[String, String](props)

    def run() = {
        consumer.subscribe(Collections.singletonList(topic))

        val exec = new Runnable {
          override def run(): Unit = {
            while (true) {
              val records = consumer.poll(1000)
              for (record <- records) {
                println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
              }
            }
          }
        }

      Executors.newSingleThreadExecutor.execute(exec)

    }

    run();
  }

}