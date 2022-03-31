import org.apache.hadoop.hbase.TableName
import org.apache.spark.sql.{SparkSession}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
    //val batchName="CRR_CUST_META"
  val tableNames = Source.fromFile("/home/kraj21/tableNames.txt").getLines.toArray
  tableNames.foreach(println)

  import spark.sql

  val groupName="CRR_CUST"+"_META"
    var tablenameString="'"
    val comma="""','"""
    var i = 0
    for(tableName<-tableNames){if(i!=tableNames.length-1){tablenameString=tablenameString+tableName+comma;i=i+1}else{tablenameString=tablenameString+tableName+"'"}}
    print(tablenameString)
  //sql("describe formatted df2_udw_odfr2s05.udw_consumer_link").show()
    val df=sql (s"""select * from df2_udw_odfr2s05.udw_consumer_link_meta where groupname='$groupName'  and tablename in ($tablenameString) """)
    //df.show()
    val Row=df("key")
    val resultRows=df.collect()
    val consumerLinkKeys= ArrayBuffer[String]()
for(Row<-resultRows){consumerLinkKeys+=(Row(0).toString())}
 val confHBase : Configuration = HBaseConfiguration.create()
    val connHBase : Connection = ConnectionFactory.createConnection(confHBase)
    val pTablePath: String = "/datalake/optum/optuminsight/udw/d04/d_mtables/udw_consumer_link_meta"
    val table=connHBase.getTable(TableName.valueOf(pTablePath))
    for (key<-consumerLinkKeys) {val putHBaseConsumerLinkMeta: Put = new Put(Bytes.toBytes(key));putHBaseConsumerLinkMeta.addColumn(Bytes.toBytes("batchinfo"), Bytes.toBytes("ProcessCompletionStatus"), Bytes.toBytes("START"));table.put(putHBaseConsumerLinkMeta)}
    connHBase.close()
