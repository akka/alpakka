/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.mongodb.ObservableToPublisher
import akka.stream.scaladsl.Source
import org.mongodb.scala.Observable

object MongoSource {

  def apply[T](query: Observable[T]): Source[T, NotUsed] =
    Source.fromPublisher(ObservableToPublisher(query))

}
