package com.bigdata.jobs

import org.apache.spark.{SparkConf, SparkContext}

object HelloScala extends App {
  println("Hello Scala")

  //Create a SparkContext to initialize Spark
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Word Count")
  val sc = new SparkContext(conf)

  val nameNode = "hdfs://sandbox-hdp.hortonworks.com:8020"
  // Load the text into a Spark RDD, which is a distributed representation of each line of text
  val textFile = sc.textFile(nameNode + "/tmp/shakespeare.txt")

  //word count
  val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)
  System.out.println("Total words: " + counts.count());
  counts.saveAsTextFile(nameNode + "/tmp/shakespeareWordCount");
}
