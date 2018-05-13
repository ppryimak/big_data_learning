package com.bigdata

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.bigdata.StatusJsonProducer.{STATUS_URL, getOriginalStatusJson, statusTransformer}
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object StatusAvroProducer {

  val STATUS_SCHEMA =
    """{
                            "fields": [
                                { "name": "station_id", "type": "string" },
                                { "name": "num_bikes_available", "type": "int" },
                                { "name": "num_docks_available", "type": "int" },
                                { "name": "last_reported", "type": "long" }
                            ],
                            "name": "status",
                            "type": "record"
                        }"""

  def main(args: Array[String]): Unit = {
    val cycles = args(0).toInt //number of ievents
    val topic = args(1) //topic
    val brokers = args(2) //"sandbox-hdp.hortonworks.com:6667"
    val pause = args(3).toLong //fake pause between evetns

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put("schema.registry.url", "http://sandbox-hdp.hortonworks.com:8081")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "StatusAvroProducer")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    // props.put("acks", "1")

    val producer = new KafkaProducer[String, GenericData.Record](props)

    val parser = new Parser
    val schema = parser.parse(STATUS_SCHEMA)
    //val recordInjection:Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

    for (nEvents <- Range(0, cycles)) {
      val statusJson = getOriginalStatusJson(STATUS_URL)
      val statuses = statusTransformer.transform(statusJson)
      println("SENDING " + statuses.size + "to topic " + topic);
      statuses.foreach (status => {
        val avroRecord = statusTransformer.toAvro(status, schema)
        println(avroRecord)
        //val key = "{\"station_id\":\"" + status.stationId + "\"}"
        val data = new ProducerRecord[String, GenericData.Record](topic, status.stationId, avroRecord)
        producer.send(data)
        //println(s"${ack.toString} written to partition ${ack.partition.toString}")
      })
    }

    println("SENT");
    TimeUnit.SECONDS.sleep(pause);
  }

  println("DONE");
}

