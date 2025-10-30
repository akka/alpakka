/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.mongodb.javadsl

import akka.NotUsed
import akka.stream.javadsl.Source
import org.reactivestreams.Publisher

object MongoSource {

  def create[T](query: Publisher[T]): Source[T, NotUsed] =
    Source.fromPublisher(query)

}
