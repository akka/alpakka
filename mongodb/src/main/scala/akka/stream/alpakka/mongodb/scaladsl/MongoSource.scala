/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mongodb.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.reactivestreams.Publisher

object MongoSource {

  def apply[T](query: Publisher[T]): Source[T, NotUsed] =
    Source.fromPublisher(query)

}
