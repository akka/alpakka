/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.mongodb.ObservableToPublisher
import akka.stream.scaladsl.Source
import org.mongodb.scala.{Document, Observable}

object MongoSource {

  def apply(query: Observable[Document]): Source[Document, NotUsed] =
    Source.fromPublisher(ObservableToPublisher(query))

}
