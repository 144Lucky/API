
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

object Cassandra2Spark {

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "172.16.0.33")
//      .set("spark.cassandra.auth.username", "cassandra")
//      .set("spark.cassandra.auth.password", "cassandra")

    lazy val sc = new SparkContext("local", "test", conf)
    val tbName = "mstf"
    val rdd = sc.cassandraTable("test", tbName)

    val cfName = "f1"
    val rk = "uid"
    val columnArray = Array( "close","day", "high", "low", "open", "volume")

    toHBase.RowToHBase(rdd,tbName,cfName,rk,columnArray)


    //rdd.collect.foreach(println)
    //println(rdd.map(_.getInt("value")).sum)


  }

}
