import org.apache.spark.{SparkContext, SparkConf}
import it.nerdammer.spark.hbase._


object SqlOnHbase {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.hbase.host", "172.16.0.30")
    val sc = new SparkContext("local", "SqlOnHBase", sparkConf)

/*    val rdd = sc.parallelize(1 to 100)
      .map(i => (i.toString, i+1, "Hello"))

    rdd.toHBaseTable("mytable")
      .toColumns("column1", "column2")
      .inColumnFamily("mycf")
      .save()*/

    val hBaseRDD = sc.hbaseTable[(String, Int, String)]("mytable")
      .select("column1", "column2")
      .inColumnFamily("mycf")

    hBaseRDD.foreach(t => {
      if(t._1.nonEmpty) println(t._1)
    })

  }
}
