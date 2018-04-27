package com.bigdata.example

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SparkSession}

object CsvToParquet {
  def main(args: Array[String]): Unit = {
    println("START " + CsvToParquet.getClass)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("CSVtoParquet").getOrCreate();

    val sqlContext = spark.sqlContext;
    val schema = StructType(Array(
      StructField("station_id", IntegerType, false),
      StructField("bikes_available", IntegerType, false),
      StructField("docks_available", IntegerType, false),
      StructField("time", DateType, false)))

    convert(sqlContext, "/bikein/status.csv", schema, "status")


    println("END " + CsvToParquet.getClass)
  }

  def convert(sqlContext: SQLContext, filename: String, schema: StructType, tablename: String) {
    val df = sqlContext.read
      .format("csv")
      .schema(schema)
      //.option("delimiter", "|")
      //.option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("header","false")
      .load(filename)
    // now simply write to a parquet file
    df.write.parquet("/rawarea/" + tablename)
  }
}
