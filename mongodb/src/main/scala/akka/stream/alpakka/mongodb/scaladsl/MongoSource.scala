/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.reactivestreams.Publisher

object MongoSource {

  def apply[T](query: Publisher[T]): Source[T, NotUsed] =
    Source.fromPublisher(query)

}
