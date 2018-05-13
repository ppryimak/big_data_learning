package com.bigdata

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsValue, _}

class StatusTransformer {

  def transform(statusJson:String): Vector[Status] = {

    val jsonValue: JsValue = statusJson.parseJson

    val jsonValueData = jsonValue.asJsObject.fields.get("data").get

    val jsonValueStations = jsonValueData.asJsObject.fields.get("stations").get
    val stationsArray: JsArray = jsonValueStations.asInstanceOf[JsArray]

    val stationsStatusJson: Vector[JsValue] = stationsArray.elements;

    val statuses:Vector[Status] = stationsStatusJson
      .map(convertJsonStrToStatusObj);
    statuses
  }

  def toJson(c: Status) = JsObject(
    "station_id" -> JsString(c.stationId),
    "num_bikes_available" -> JsNumber(c.docksAvailable),
    "num_docks_available" -> JsNumber(c.bikeAvailable),
    "last_reported" -> JsNumber(c.lastReported)
  )

  def toAvro(c:Status, schema:Schema):GenericData.Record = {
    import org.apache.avro.generic.GenericData
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("station_id", c.stationId)
    avroRecord.put("num_bikes_available", c.bikeAvailable)
    avroRecord.put("num_docks_available", c.docksAvailable)
    avroRecord.put("last_reported", c.lastReported)
    avroRecord
  }

  private def convertJsonStrToStatusObj(statusJson:JsValue) ={
    statusJson.asJsObject.getFields("station_id", "num_docks_available", "num_bikes_available", "last_reported") match {
      case Seq(stationId, docksAvailable, bikeAvailable, lastReported) ⇒ Status(
        stationId.convertTo[String],
        bikeAvailable.convertTo[Int],
        docksAvailable.convertTo[Int],
        lastReported.convertTo[Long]

      )
      case other ⇒ deserializationError("Cannot deserialize ProductItem: invalid input. Raw input: " + other)
    }
  }

  private def longToDateStr(dateLong:Long) = {
    val df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    val date = new Date(dateLong * 1000)
    df.format(date)
  }


}

case class Status (stationId: String, bikeAvailable:Int, docksAvailable:Int, lastReported:Long);

