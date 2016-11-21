
import org.apache.spark.{SparkContext, SparkConf}
import com.mongodb.spark._
import org.apache.hadoop.conf.Configuration

object Mongo2Spark extends App {
  // Set up the configuration for reading from MongoDB.
  val mongoConfig = new Configuration()
  mongoConfig.set("mongo.input.uri",
    "mongodb://data28:27017/marketdata.minibars")

  val sparkConf = new SparkConf()
  sparkConf.set("spark.mongodb.input.uri", "mongodb://data28:27017/marketdata.minibars")
  val sc = new SparkContext("local", "Mongo2Spark", sparkConf)
//  val sqlContext = SQLContext.getOrCreate(sc)

  val rdd = MongoSpark.load(sc)
  val columnArray = Array("Symbol", "Day", "Open", "High", "Low", "Close", "Volume")
  val tbName = "minibars"
  val cfName = "f1"

  toHBase.DocToHBase(rdd,tbName,cfName,columnArray)

// println(rdd.count)
// println(rdd.first.toJson)

  //val df = MongoSpark.load(sqlContext)
  //df.printSchema()

/*  case class Character(name: String, age: Int)
  val explicitDF = MongoSpark.load[Character](sqlContext)
  explicitDF.printSchema()*/


/*  // Create an RDD backed by the MongoDB collection.
  val documents = sc.newAPIHadoopRDD(
    mongoConfig,                // Configuration
    classOf[com.mongodb.hadoop.MongoInputFormat],  // InputFormat
    classOf[Object],            // Key type
    classOf[BSONObject])     // Value type*/

}
