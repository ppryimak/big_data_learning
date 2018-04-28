package com.bigdata

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.hdfs.inotify.Event
import org.apache.hadoop.hdfs.inotify.Event.EventType


object HdfsEventsListener {

  def main(args: Array[String]): Unit = {
    println("Hello from main of class")

    var lastReadTxid = 0L

    if (args.length > 1) {
      lastReadTxid = args(1).toLong
    };

    System.out.println("lastReadTxid = " + lastReadTxid)

    val admin = new HdfsAdmin(URI.create(args(0)), new Configuration)

    val eventStream = admin.getInotifyEventStream(lastReadTxid)

    while ( {
      true
    }) {
      val batch = eventStream.take
      //System.out.println("TxId = " + batch.getTxid)
      for (event <- batch.getEvents) {
        //System.out.println("event type = " + event.getEventType)
        event.getEventType match {
          case EventType.CREATE =>
            val createEvent = event.asInstanceOf[Event.CreateEvent]
            if(createEvent.getPath.startsWith("/bikein/status")) { //TODO: add support for all files
              System.out.println("  path = " + createEvent.getPath)
              System.out.println("  owner = " + createEvent.getOwnerName)
              System.out.println("  ctime = " + createEvent.getCtime)
              executeCreated(createEvent.getPath, createEvent.getCtime)
            }
          case EventType.UNLINK =>
          case EventType.APPEND =>
          case EventType.CLOSE =>
          case EventType.RENAME =>
          case _ =>
        }
      }
    }
  }

  def executeCreated(path: String, time: Long) = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = df.format(time.toLong)

    val p = new ProcessBuilder("/bin/bash","created.sh", path, date)
    val p2 = p.start()
    val br = new BufferedReader(new InputStreamReader(p2.getInputStream()))

    var line:String = ""
    while ({line = br.readLine();  line!= null}) {
      println(line)
    }
  }

}
