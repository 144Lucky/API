import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HTable, ConnectionFactory, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.ToolRunner
import org.apache.spark.{SparkConf, SparkContext}

object Spark2HBase {
  def main(args: Array[String]) {
    //创建sparkcontext,用默认的配置
    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "Spark2HBase", sparkConf)
    //hbase的列族
    val columnFamily1 = "f1"
    //hbase的默认配置文件
    val conf = HBaseConfiguration.create()
    conf.set("hbase.client.pause", "3000")
    conf.set("hbase.client.retries.number", "5")
    conf.set("hbase.client.operation.timeout", "60000")
    conf.set("hbase.zookeeper.quorum", "172.16.0.30")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    //当前时间
    val date = new Date().getTime
    //初始化RDD,用 sc.parallelize 生成一个RDD
    val sourceRDD = sc.parallelize(Array(
      (Bytes.toBytes("40"), //41是rowkey
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo1"))), //分别设置family  colum  和 value
      (Bytes.toBytes("40"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("foo2.b"))),
      (Bytes.toBytes("41"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("bar.1"))),
      (Bytes.toBytes("41"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("d"), Bytes.toBytes("bar.2"))),
      (Bytes.toBytes("50"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo2.a"))),
      (Bytes.toBytes("50"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("c"), Bytes.toBytes("foo2.c"))),
      (Bytes.toBytes("51"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo3"))),
      (Bytes.toBytes("52"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo.1"))),
      (Bytes.toBytes("52"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("foo.2")))))

    val rdd = sourceRDD.map(x => {
      //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
      //KeyValue的实例为value
      //rowkey
      val rowKey = x._1
      val family = x._2._1
      val colum = x._2._2
      val value = x._2._3
      (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
    })

    //生成的HFile的临时保存路径
    val stagingFolder = "/user/hbase/spark/"
    //将日志保存到指定目录
    rdd.saveAsNewAPIHadoopFile(stagingFolder,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)
    //此处运行完成之后,在stagingFolder会有我们生成的Hfile文件


    //开始即那个HFile导入到Hbase,此处都是hbase的api操作
    // val load = new LoadIncrementalHFiles(conf)
    //hbase的表名
    val tableName = "spark2hbase"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn = ConnectionFactory.createConnection(conf)
    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val htable = new  HTable(conf,tableName)
    try {
      //获取hbase表的region分布
      val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
      val list= List( regionLocator.getAllRegionLocations)
      list.foreach(println)
      //创建一个hadoop的mapreduce的job
      val job = Job.getInstance(conf)
      //设置job名称
      job.setJobName("DumpFile")
      //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])
      //配置HFileOutputFormat2的信息
      HFileOutputFormat2.configureIncrementalLoad(job, table,regionLocator)

      //开始导入
      val load = new LoadIncrementalHFiles(conf)
      load.doBulkLoad(new Path(stagingFolder), htable)

      //val Args= Array(stagingFolder,tableName)

      // ToolRunner.run(new LoadIncrementalHFiles(conf),Args)
    } finally {
      table.close()
      conn.close()
    }
  }
}
