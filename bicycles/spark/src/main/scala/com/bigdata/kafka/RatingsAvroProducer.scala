package com.bigdata.kafka

import java.util.Properties

import com.bigdata.jobs.Rate
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class RatingsAvroProducer {

  val topic = "rating"
  val RATING_SCHEMA =
    """{
                            "fields": [
                                { "name": "top3", "type": "string" },
                                { "name": "last3", "type": "string" }
                            ],
                            "name": "rating",
                            "type": "record"
                        }"""

  val producer = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667")
    props.put("schema.registry.url", "http://sandbox-hdp.hortonworks.com:8081")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "RatingAvroProducer")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")

    new KafkaProducer[String, GenericData.Record](props)
  }

  def sendRating(top3:Array[Rate], last3:Array[Rate]): Unit = {
    val parser = new Parser
    val schema = parser.parse(RATING_SCHEMA)

    val avroRecord = toAvro(top3, last3, schema)
    println("Sending rating")
    println(avroRecord)
    val data = new ProducerRecord[String, GenericData.Record](topic, getNowStr(), avroRecord)
    producer.send(data)
    println("Rating sent")
  }

  def toAvro(top3:Array[Rate], last3:Array[Rate], schema:Schema):GenericData.Record = {
    import org.apache.avro.generic.GenericData
    val avroRecord = new GenericData.Record(schema)

    val top3Str:String = top3(0).stationId + " - " +top3(0).rate + "; " +
                         top3(1).stationId + " - " +top3(1).rate + "; " +
                        top3(2).stationId + " - " +top3(2).rate

    val last3Str:String = last3(0).stationId + " - " +last3(0).rate + "; " +
      last3(1).stationId + " - " +last3(1).rate + "; " +
      last3(2).stationId + " - " +last3(2).rate

    avroRecord.put("top3", top3Str)
    avroRecord.put("last3", last3Str)
    avroRecord
  }

  def getNowStr(): String = {
    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter
    val now = LocalDateTime.now
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    now.format(formatter)
  }
}
