package akka.stream.alpakka.mongodb

import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.{MongoClient, MongoDatabase}

object Mongo {
  private val config: Config = ConfigFactory.load(getClass.getClassLoader, "application.conf")

  private val dbName: String = config.getString("mongo.name")
  private val dbAddress: String = config.getString("mongo.host")
  private val dbPort: String = config.getString("mongo.port")

  val client: MongoClient = MongoClient(s"mongodb://$dbAddress:$dbPort")

  val db: MongoDatabase = client.getDatabase(dbName)
}
