package akka.stream.alpakka.mongodb.scaladsl

import akka.stream.alpakka.mongodb.Implicits._
import akka.stream.scaladsl.Source
import org.mongodb.scala.{Document, Observable}

object MongoSource {

  def apply(query: Observable[Document]) ={
    Source.fromPublisher(query)
  }

}
