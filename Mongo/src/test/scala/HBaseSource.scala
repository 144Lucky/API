/**
  * Created by kojo on 2016/10/11.
  */
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.{SparkConf, SparkContext}

case class HBaseRecord(
                        col0: String,
                        col1: Boolean,
                        col2: Double,
                        col3: Float,
                        col4: Int,
                        col5: Long,
                        col6: Short,
                        col7: String,
                        col8: Byte)

object HBaseRecord {
  def apply(i: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

object HBaseSource {
  val cat = s"""{
                |"table":{"namespace":"default", "name":"shcExampleTable"},
                |"rowkey":"key",
                |"columns":{
                |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
                |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                |}
                |}""".stripMargin

  val cat1 = s"""{
                 |"table":{"namespace":"default", "name":"mytable"},
                 |"rowkey":"key",
                 |"columns":{
                 |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                 |"col1":{"cf":"mycf", "col":"column1", "type":"int"},
                 |"col2":{"cf":"mycf", "col":"column2", "type":"string"}
                 |}
                 |}""".stripMargin

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "SHC", sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    def withCatalog(catalog: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

/*    val df = withCatalog(cat1)
    df.registerTempTable("mytable")
    sqlContext.sql("select count(col0) from mytable").show*/

/*    val data1 = (101 to 200)
      .map(i => (i.toString, i+1, "Hello"))

    sc.parallelize(data1).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat1))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .append()*/

    val data = (300 to 400).map { i =>
      HBaseRecord(i)
    }

    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

}
