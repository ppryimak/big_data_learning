package com.bigdata

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred._


class MPTripsMapper extends MapReduceBase with Mapper[Object, Text, Text, IntWritable] {
  private val ONE = new IntWritable(1)

  override //
  def map(key: Object, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {

    val valueString = value.toString
    val row = valueString.split(",")
    //key format: start_station_id-end_station_id
    val key = row(4) + "-" + row(7);
    output.collect(new Text(key), ONE)
  }
}


class MPTripsReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
  override //
  def reduce(key: Text, values: util.Iterator[IntWritable], //
             output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
    var frequency = 0
    while ( {
      values.hasNext
    }) {
      val value = values.next
      frequency += value.get
    }
    output.collect(key, new IntWritable(frequency))
  }
}


object MPTripsDriver {
  def main(args: Array[String]): Unit = {
    println("START MAP REDUCE for " + MPTripsDriver.getClass.getName)

    val client = new JobClient
    val jobConf = new JobConf(classOf[MPTripsMapper])

    jobConf.setJobName("MostPopularTrips")

    jobConf.setOutputKeyClass(classOf[Text])
    jobConf.setOutputValueClass(classOf[IntWritable])

    jobConf.setMapperClass(classOf[MPTripsMapper])
    jobConf.setReducerClass(classOf[MPTripsReducer])

    jobConf.setInputFormat(classOf[TextInputFormat])
    jobConf.setOutputFormat(classOf[TextOutputFormat[_, _]])

    FileInputFormat.setInputPaths(jobConf, new Path(args(0)))
    FileOutputFormat.setOutputPath(jobConf, new Path(args(1)))

    client.setConf(jobConf)
    try
      JobClient.runJob(jobConf)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }

    println("END MAP REDUCE for " + MPTripsDriver.getClass.getName)
  }


}
