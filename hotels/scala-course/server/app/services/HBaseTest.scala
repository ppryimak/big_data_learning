package services

import javax.inject.Singleton
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import play.api.Logger

@Singleton
class HBaseTest {

  def createTableOrOverwrite(admin: Admin, table: HTableDescriptor): Unit = {
    if (admin.tableExists(table.getTableName)) {
      admin.disableTable(table.getTableName)
      admin.deleteTable(table.getTableName)
    }
    admin.createTable(table)
  }

  def printRow(result: Result) = {
    val cells = result.rawCells();
    print(Bytes.toString(result.getRow) + " : ")
    for (cell <- cells) {
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
      val col_value = Bytes.toString(CellUtil.cloneValue(cell))
      print("(%s,%s) ".format(col_name, col_value))
    }
    println()
  }

  def test() = {
    val usrDir = System.getProperty("user.dir");
    Logger.debug(s"User.dir is set to $usrDir")


    val conf: Configuration = HBaseConfiguration.create()

    //TODO: move to properties
    val ZOOKEEPER_QUORUM = "sandbox-hdp.hortonworks.com"
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
    conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val connection = ConnectionFactory.createConnection(conf)
    //val tableName = TableName.valueOf( Bytes.toBytes("emostafa:test_table") )


    val admin = connection.getAdmin();



    val newTable: TableName = TableName.valueOf("hbase_demo")
    val tDescriptor: HTableDescriptor = new HTableDescriptor(newTable)
    //tDescriptor.addFamily(new HColumnDescriptor("CF1").setCompressionType(Algorithm.SNAPPY))
    tDescriptor.addFamily(new HColumnDescriptor("CF1"))
    createTableOrOverwrite(admin, tDescriptor)

    Logger.debug("Table created")

    val table = connection.getTable(newTable)


    try {


      // Put example - used for updates also
      var put = new Put(Bytes.toBytes("row1"))
      put.addColumn(Bytes.toBytes("CF1"), Bytes.toBytes("test_column_name"), Bytes.toBytes("test_value"))
      put.addColumn(Bytes.toBytes("CF1"), Bytes.toBytes("test_column_name2"), Bytes.toBytes("test_value2"))
      table.put(put)

      // Get example
      println("Get Example:")
      var get = new Get(Bytes.toBytes("row1"))
      var result = table.get(get)
      printRow(result)

      //Scan example
      println("Scan Example:")
      var scan = table.getScanner(new Scan())
      scan.asScala.foreach(result => {
        printRow(result)
      })

    } catch {
      case e: Exception => {
        Logger.error("Error connecting to hbase: ", e);
        e.printStackTrace();
      }
    } finally {
      table.close()
      connection.close()
    }
  }
}
