package notcommit

import java.nio.charset.StandardCharsets

import com.bigdata.hbase.StatusHBaseService
import com.bigdata.streaming.Status

object HBaseUtil {
  def main(args: Array[String]): Unit = {
    val hbaseService = new StatusHBaseService
    hbaseService.scan()
    //hbaseService.removeTable(Status.tableName, new String(Status.cfDataBytes, StandardCharsets.UTF_8));
    //hbaseService.createTable(Status.tableName, new String(Status.cfDataBytes, StandardCharsets.UTF_8));
    //hbaseService.scan()


  }

}
