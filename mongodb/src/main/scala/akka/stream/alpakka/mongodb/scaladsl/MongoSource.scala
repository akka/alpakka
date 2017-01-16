package akka.stream.alpakka.mongodb.scaladsl

import akka.stream.scaladsl.Source
import org.mongodb.scala.{Document, Observable}
import play.api.libs.json.{JsObject, Json}
import akka.stream.alpakka.mongodb.Implicits._

object MongoSource {

  def apply(query: Observable[Document]) ={
    Source.fromPublisher(query).map(doc=>Json.parse(doc.toJson()).as[JsObject])
  }

}
