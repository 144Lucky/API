
import com.datastax.spark.connector.CassandraRow
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.{RDD, PairRDDFunctions}
import org.bson.Document

object toHBase {

  implicit lazy val hbaseConfig = HBaseConfig(
    "hbase.zookeeper.quorum" -> "172.16.0.30"
    ,"hbase.zookeeper.property.clientPort" -> "2181"
    ,"zookeeper.znode.parent" -> "/hbase"
  ).get

  def convert(tb : String, cf : String, rk: String, values: Map[String, String]):
  (ImmutableBytesWritable, Put)= {
    val put = new Put(Bytes.toBytes(rk))
    for ((key, value) <- values ) {
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(key), Bytes.toBytes(value))
    }

    (new ImmutableBytesWritable(Bytes.toBytes(tb)), put)
  }

  def DocToHBase(rdd : RDD[Document], tb : String, cf : String,
                 columnArray : Array[String] ) {

    new PairRDDFunctions(rdd.map {
      doc =>
        val rk = doc.getObjectId("_id").toString
        val length: Int = columnArray.length
        var mp = Map[String, String]()
        for (index <- 0 until length - 1) {
        mp += (columnArray(index) -> doc.getString(columnArray(index)))
        }
      convert(tb,cf,rk, mp)

    }).saveAsNewAPIHadoopFile("", classOf[String],classOf[String], classOf[MultiTableOutputFormat], hbaseConfig)
  }

  def RowToHBase(rdd : RDD[CassandraRow], tb : String, cf : String,
                 rowkey : String,columnArray : Array[String] ) {


    new PairRDDFunctions(rdd.map {
      row =>
        val rk = row.getString(rowkey)
        val length: Int = columnArray.length
        var mp = Map[String, String]()
        for (index <- 0 until length - 1) {
          mp += (columnArray(index) -> row.getString(columnArray(index)))
        }
        convert(tb,cf,rk, mp)

    }).saveAsNewAPIHadoopFile("", classOf[String],classOf[String], classOf[MultiTableOutputFormat], hbaseConfig)
  }

}
