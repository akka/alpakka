/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference.javadsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.reference.{scaladsl, ReferenceWriteMessage, Resource}
import akka.stream.javadsl.Flow

/**
 * Akka Stream operator factories that use resource instance from the extension.
 */
object ReferenceWithResource {
  def flow(sys: ActorSystem): Flow[ReferenceWriteMessage, ReferenceWriteMessage, NotUsed] =
    scaladsl.ReferenceWithResource.flow()(sys).asJava
}

/**
 * Akka Stream operator factories that take an external resource.
 */
object ReferenceWithExternalResource {
  def flow(r: Resource): Flow[ReferenceWriteMessage, ReferenceWriteMessage, NotUsed] =
    scaladsl.ReferenceWithExternalResource.flow()(r).asJava
}
