package akka.stream.alpakka.mongodb

import org.mongodb.scala.{MongoClient, MongoDatabase}

private[mongodb] case class MongoConnectionDetails(dbName: String,host: String, port:Int) {
  lazy val client: MongoClient = MongoClient(s"mongodb://$host:$port")

  lazy val db: MongoDatabase = client.getDatabase(dbName)
}
