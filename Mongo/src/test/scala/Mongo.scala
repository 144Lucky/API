/**
  * Created by kojo on 2016/8/17.
  */

// https://github.com/Stratio/Spark-MongoDB/blob/master/spark-mongodb-examples/src/main/scala/com/stratio/datasource/mongodb/examples/ExampleUtils.scala

import com.mongodb.casbah.Imports._

object Mongo {
  def main(args: Array[String]): Unit = {
    val mongoClient = MongoClient("data28", 27017)
    val collection = mongoClient("marketdata")("minibars")

    val allDocs = collection.find()

    println(allDocs)
    for(doc <- allDocs) println(doc)
  }
}
