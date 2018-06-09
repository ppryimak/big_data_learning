package com.bigdata.kafka

import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

case class Alert (stationId: String, alert:String);

class AlertAvroProducer {

  val topic = "alert"
  val ALERT_SCHEMA =
    """{
                            "fields": [
                                { "name": "station_id", "type": "string" },
                                { "name": "alert", "type": "string" }
                            ],
                            "name": "alert",
                            "type": "record"
                        }"""

  val producer = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667")
    props.put("schema.registry.url", "http://sandbox-hdp.hortonworks.com:8081")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "StatusAvroProducer")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    // props.put("acks", "1")

    new KafkaProducer[String, GenericData.Record](props)
  }

  def sendAlert(alert:Alert): Unit = {
    val parser = new Parser
    val schema = parser.parse(ALERT_SCHEMA)

    val avroRecord = toAvro(alert, schema)
    println("Sending alert")
    println(avroRecord)
    val data = new ProducerRecord[String, GenericData.Record](topic, alert.stationId, avroRecord)
    producer.send(data)
    println("Alert sent")
  }

  def toAvro(c:Alert, schema:Schema):GenericData.Record = {
    import org.apache.avro.generic.GenericData
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("station_id", c.stationId)
    avroRecord.put("alert", c.alert)
    avroRecord
  }
}

