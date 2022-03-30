import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
 import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
val confHBase : Configuration = HBaseConfiguration.create()
val connHBase : Connection = ConnectionFactory.createConnection(confHBase)
val putHBaseConsumerLinkMeta: Put = new Put(Bytes.toBytes("CRR_CUST_METAmemgroupcontractopt_META"))
putHBaseConsumerLinkMeta.addColumn(Bytes.toBytes("batchinfo"), Bytes.toBytes("ProcessCompletionStatus"), Bytes.toBytes("START"))
val pTablePath: String="/datalake/optum/optuminsight/udw/d04/d_mtables/udw_consumer_link_meta"
connHBase.getTable(TableName.valueOf(pTablePath)).put(putHBaseConsumerLinkMeta)
