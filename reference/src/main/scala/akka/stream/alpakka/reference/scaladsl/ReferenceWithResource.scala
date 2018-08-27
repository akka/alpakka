/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference.scaladsl
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.reference.impl.ReferenceWithResourceFlow
import akka.stream.alpakka.reference.{ReferenceWriteMessage, Resource, ResourceExt}
import akka.stream.scaladsl.Flow

/**
 * Akka Stream operator factories that use resource instance from the extension.
 */
object ReferenceWithResource {
  def flow()(implicit sys: ActorSystem): Flow[ReferenceWriteMessage, ReferenceWriteMessage, NotUsed] =
    ReferenceWithExternalResource.flow()(ResourceExt().resource)
}

/**
 * Akka Stream operator factories that take an external resource.
 */
object ReferenceWithExternalResource {
  def flow()(implicit r: Resource): Flow[ReferenceWriteMessage, ReferenceWriteMessage, NotUsed] =
    Flow.fromGraph(new ReferenceWithResourceFlow(r))
}
