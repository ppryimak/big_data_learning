package com.bigdata

import java.io.FileWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object MainScala {
  def main(args: Array[String]): Unit = {
    val header = "station_id,bikes_available,docks_available,time";
    println(header)

    var startTime = LocalDateTime.of(2017, 5, 5, 5, 5, 5);
    val numIterrations = 10;
    val r = scala.util.Random
    val stations = Array(1, 2, 3, 4, 5, 6, 7)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")


    val fw = new FileWriter("status.csv")
    try {
      fw.write(header)
      fw.write(System.getProperty("line.separator"))

      for (i <- 1 to numIterrations) {

        stations.foreach(x => {
          //if (r.nextInt() % 2 == 0) {
          val bikes_available = r.nextInt(15)
          val docks_available = 15 - bikes_available
          startTime = startTime.plusSeconds(i)
          val res = "%s,%s,%s,%s".format(x, bikes_available, docks_available, startTime.format(formatter))
          println(res)
          fw.write(res)
          fw.write(System.getProperty("line.separator"))
          //}
        })
        startTime = startTime.plusMinutes(1)
      }

    }
    finally fw.close()

  }
}
