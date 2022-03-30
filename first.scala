package com.sundogsoftware.spark
import org.apache.hadoop.hbase.TableName
import org.apache.spark.sql.{SparkSession}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import scala.io.Source

object Datarefresh {
  def main(args:Array[String]):Unit={ 

  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .config("spark.sql.warehouse.dir", "/datalake/uhclake/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
  //get tables and batchname from command line
    val batchName=args(0)
  val tableNames = Source.fromFile(args(1)).getLines.toArray
  tableNames.foreach(println)

  import spark.sql

  val groupName=batchName+"_META"
    var tablenameString="'"
    val comma="""','"""
    var i = 0
    for(tableName<-tableNames){
      if(i!=tableNames.length-1){
        tablenameString+=tableName+comma
        i=i+1
      }else{
        tablenameString+=tableName+"'"
      }
    }
    print(tablenameString)
  //sql("describe formatted df2_udw_odfr2s05.udw_consumer_link").show()
    val df=sql (s"""select * from df2_udw_odfr2s05.udw_consumer_link_meta where groupname='$groupName'  and tablename in ($tablenameString) """)
    //df.show()
    val Row=df("key")
    val resultRows=df.collect()
    var consumerLinkKeys:Array[String] = new Array[String](resultRows.length)
    for(Row<-resultRows){
      consumerLinkKeys:+Row(0).toString()
    }

    val confHBase : Configuration = HBaseConfiguration.create()
    val connHBase : Connection = ConnectionFactory.createConnection(confHBase)
    val pTablePath: String = "/datalake/optum/optuminsight/udw/d04/d_mtables/udw_consumer_link_meta"
    val table=connHBase.getTable(TableName.valueOf(pTablePath))
    for (key<-consumerLinkKeys) {
      val putHBaseConsumerLinkMeta: Put = new Put(Bytes.toBytes(key))
      putHBaseConsumerLinkMeta.addColumn(Bytes.toBytes("batchinfo"), Bytes.toBytes("ProcessCompletionStatus"), Bytes.toBytes("START"))
      table.put(putHBaseConsumerLinkMeta)
    }
    connHBase.close()
    spark.stop()
  }
}
