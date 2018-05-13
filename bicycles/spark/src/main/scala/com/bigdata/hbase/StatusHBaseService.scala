package com.bigdata.hbase

import java.nio.charset.StandardCharsets

import com.bigdata.streaming.Status
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

class StatusHBaseService {

  def createTable(tableName: String, columnFamily:String): Unit = {

    val newTable: TableName = TableName.valueOf(tableName)
    val table: HTableDescriptor = new HTableDescriptor(newTable)
    //table.addFamily(new HColumnDescriptor("CF1").setCompressionType(Algorithm.SNAPPY))
    table.addFamily(new HColumnDescriptor(columnFamily))

    if (!ADMIN.tableExists(table.getTableName)) {
      ADMIN.createTable(table)
      println(Status.tableName + " was created")
    }
    println(Status.tableName + " already exists")
  }

  def removeTable(tableName: String, columnFamily:String): Unit = {

    val tableNameC: TableName = TableName.valueOf(tableName)
    val table: HTableDescriptor = new HTableDescriptor(tableNameC)
    table.addFamily(new HColumnDescriptor(columnFamily))

    if (ADMIN.tableExists(table.getTableName)) {
      ADMIN.disableTable(table.getTableName)
      ADMIN.deleteTable(table.getTableName)
    }
  }

  def printRow(result: Result) = {
    val cells = result.rawCells();
    print(Bytes.toString(result.getRow) + " : ")
    for (cell <- cells) {
      val colName = Bytes.toString(CellUtil.cloneQualifier(cell))
      var colValue = "";
      if(colName =="station_id") {
        colValue = Bytes.toString(CellUtil.cloneValue(cell))
      } else if(colName =="last_updated") {
        colValue = Bytes.toLong(CellUtil.cloneValue(cell)).toString
      } else {
        colValue = Bytes.toInt(CellUtil.cloneValue(cell)).toString
      }
      print("(%s,%s) ".format(colName, colValue))
    }
    println()
  }

  private val CONF: Configuration = HBaseConfiguration.create()

  //TODO: move to properties or input args
  val ZOOKEEPER_QUORUM = "sandbox-hdp.hortonworks.com"
  CONF.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
  CONF.set("zookeeper.znode.parent", "/hbase-unsecure");
  CONF.set("hbase.zookeeper.property.clientPort", "2181")

  val CONNECTION = ConnectionFactory.createConnection(CONF)
  val ADMIN = CONNECTION.getAdmin();

  createTable(Status.tableName, new String(Status.cfDataBytes, StandardCharsets.UTF_8));



  val table = CONNECTION.getTable(TableName.valueOf(Status.tableName))



  def put(statuses:List[Status]) {
    val puts = statuses.map(Status.convertToPut).asJava;
    table.put(puts)
    println("put successful")

  }

  def put(status:Status) {
    val put = Status.convertToPut(status);
    table.put(put)
    println("put successful")

  }

  def scan(): Unit = {
    println("Start Scan:")
    var scan = table.getScanner(new Scan())
    scan.asScala.foreach(result => {
      printRow(result)
    })
    println("End Scan:")
  }
}
