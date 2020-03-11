/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source
import org.reactivestreams.Publisher

object MongoSource {

  def create[T](query: Publisher[T]): Source[T, NotUsed] =
    Source.fromPublisher(query)

}
