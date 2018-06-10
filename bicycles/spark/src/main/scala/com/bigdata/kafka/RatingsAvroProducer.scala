package com.bigdata.kafka

import java.util.Properties

import com.bigdata.jobs.Rate
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class RatingsAvroProducer {

  val topic = "rating1"
  val RATING_SCHEMA =
    """{
                            "fields": [
                                { "name": "top3", "type": "string" },
                                { "name": "last3", "type": "string" },
                                { "name": "count", "type": ["null", "long"] }
                            ],
                            "name": "rating_scheme",
                            "type": "record"
                        }"""

  def sendRating(top3:Array[Rate], last3:Array[Rate], count: Long): Unit = {
    val parser = new Parser
    val schema = parser.parse(RATING_SCHEMA)

    val avroRecord = toAvro(top3, last3, count, schema)
    println("Sending rating")
    println(avroRecord)
    val data = new ProducerRecord[String, GenericData.Record](topic, getNowStr(), avroRecord)
    RatingsAvroProducer.PRODUCER.send(data)
    println("Rating sent")
  }

  def toAvro(top3:Array[Rate], last3:Array[Rate], count:Long, schema:Schema):GenericData.Record = {
    import org.apache.avro.generic.GenericData
    val avroRecord = new GenericData.Record(schema)

    val top3Str:String = top3(0).stationId + " - " + format(top3(0).rate) + "; " +
                         top3(1).stationId + " - " + format(top3(1).rate) + "; " +
                        top3(2).stationId + " - " + format(top3(2).rate)

    val last3Str:String = last3(0).stationId + " - " + format(last3(0).rate) + "; " +
      last3(1).stationId + " - " + format(last3(1).rate) + "; " +
      last3(2).stationId + " - " + format(last3(2).rate)

    avroRecord.put("top3", top3Str)
    avroRecord.put("last3", last3Str)
    avroRecord.put("count", count)
    avroRecord
  }

  def getNowStr(): String = {
    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter
    val now = LocalDateTime.now
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    now.format(formatter)
  }

  def format(num:Double): String = {
    BigDecimal(num).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
  }
}

object RatingsAvroProducer {
  val PRODUCER = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667")
    props.put("schema.registry.url", "http://sandbox-hdp.hortonworks.com:8081")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "RatingAvroProducer")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")

    new KafkaProducer[String, GenericData.Record](props)
  }
}
