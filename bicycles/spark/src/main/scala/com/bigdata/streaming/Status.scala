package com.bigdata.streaming

import java.text.SimpleDateFormat
import java.util.Date

import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsValue, _}


case class Status (stationId: String, bikeAvailable:Int, docksAvailable:Int, lastReported:String);

object Status {

  def parseFromJson(statusJson:String): Vector[Status] = {
    val jsonValue: JsValue = statusJson.parseJson
    val jsonValueData = jsonValue.asJsObject.fields.get("data").get
    val jsonValueStations = jsonValueData.asJsObject.fields.get("stations").get
    val stationsArray: JsArray = jsonValueStations.asInstanceOf[JsArray]

    val stationsStatusJson: Vector[JsValue] = stationsArray.elements;

    stationsStatusJson.map(convertJsonStrToStatusObj(_));
  }

  private def convertJsonStrToStatusObj(statusJson:JsValue) ={
    statusJson.asJsObject.getFields("station_id", "num_docks_available", "num_bikes_available", "last_reported") match {
      case Seq(stationId, docksAvailable, bikeAvailable, lastReported) ⇒ Status(
        stationId.convertTo[String],
        bikeAvailable.convertTo[Int],
        docksAvailable.convertTo[Int],
        longToDateStr(lastReported.convertTo[Long])

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
