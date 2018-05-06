package com.bigdata.hbase

import com.bigdata.streaming.Status
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

class StatusHBaseService {

  def createTable(tableName: String, columnFamily:Array[Byte]): Unit = {

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

  def removeTable(tableName: String, columnFamily:Array[Byte]): Unit = {

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
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
      var col_value = "";
      if(col_name =="station_id") {
        col_value = Bytes.toString(CellUtil.cloneValue(cell))
      } else {
        col_value = Bytes.toInt(CellUtil.cloneValue(cell)).toString
      }
      print("(%s,%s) ".format(col_name, col_value))
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

  createTable(Status.tableName, Status.cfDataBytes);



  val table = CONNECTION.getTable(TableName.valueOf(Status.tableName))



  def put(statuses:List[Status]) {
    val puts = statuses.map(Status.convertToPut).asJava;
    table.put(puts)
    println("put successful")

  }

  def scan(): Unit = {
    println("Scan Example:")
    var scan = table.getScanner(new Scan())
    scan.asScala.foreach(result => {
      printRow(result)
    })
  }
}
