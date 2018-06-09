package com.bigdata.jobs

import com.bigdata.kafka.RatingsAvroProducer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

case class Rate(stationId:String, rate:Double)

object BatchJob {

  def main(args: Array[String]): Unit = {
    println("START " + CsvToParquet.getClass)

    val spark = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .appName("CSVtoParquet").getOrCreate();
    import spark.implicits._

    val sqlContext = spark.sqlContext;

    val hiveReports = sqlContext.sql("select station_id, num_bikes_available as bikes_available, from_unixtime(last_reported, 'yyyy-MM-dd hh:mm:ss') as last_reported from default.batch123")
    hiveReports.show();
    val df = hiveReports.groupBy($"station_id", window($"last_reported", "2 minutes")).agg(count("bikes_available").alias("records_count"),sum("bikes_available").alias("bikes_available"));
    df.show(100, false);
    df.createOrReplaceTempView("status")
    val res = sqlContext.sql("select station_id, sum(bikes_available)/sum(records_count) bikes_rating from status group by station_id order by bikes_rating");

    val top3rows:Array[Row] = res.head(3)
    val top3:Array[Rate] = top3rows.map(x=>new Rate(x.getAs("station_id"), x.getAs("bikes_rating")))
    top3.foreach(println);

    val last3rows = res.orderBy($"bikes_rating".desc).head(3)
    val last3:Array[Rate] = last3rows.map(x=>new Rate(x.getAs("station_id"), x.getAs("bikes_rating")))
    last3.foreach(println);

    new RatingsAvroProducer().sendRating(top3, last3)
    println("END " + BatchJob.getClass)
  }


}
