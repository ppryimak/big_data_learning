package com.bigdata

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ArrayWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred._


class LackOfBicyclesMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, Text] {
  //private val ONE = new IntWritable(1)

  override //
  def map(l_key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit = {

    if (l_key.get == 0) { //skip header
      return;
    }

    val valueString = value.toString
    val row: Array[String] = valueString.split(",")
    //key format: start_station_id-end_station_id
    val key = row(0);
    val valueStr = ',' + row(1) + ",1";

    output.collect(new Text(key), new Text(valueStr))
  }
}


class LackOfBicyclesReducer extends MapReduceBase with Reducer[Text, Text, Text, Text] {
  override //
  def reduce(key: Text, values: util.Iterator[Text], //
             output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
    var frequency = 0
    var numBikes = 0
    while ( {
      values.hasNext
    }) {
      val valueString = values.next.toString
      val arr: Array[String] = valueString.split(",")

      numBikes += arr(0).toInt
      frequency += arr(1).toInt
    }
    val valueStr = ',' + numBikes.toString + ',' + frequency.toString;

    output.collect(key, new Text(valueStr))
  }
}



object LackOfBicycles {

  def main(args: Array[String]): Unit = {
    println("START MAP REDUCE for " + LackOfBicycles.getClass.getName)

    val client = new JobClient
    val jobConf = new JobConf(classOf[LackOfBicyclesMapper])

    jobConf.setJobName("LackOfBicycles")

    jobConf.setOutputKeyClass(classOf[Text])
    jobConf.setOutputValueClass(classOf[Text])

    jobConf.setMapperClass(classOf[LackOfBicyclesMapper])
    jobConf.setReducerClass(classOf[LackOfBicyclesReducer])

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

    println("END MAP REDUCE for " + LackOfBicycles.getClass.getName)
  }
}
