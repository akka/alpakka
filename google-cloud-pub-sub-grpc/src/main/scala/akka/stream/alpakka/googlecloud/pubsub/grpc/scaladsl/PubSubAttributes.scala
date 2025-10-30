/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl

import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.Attributes.Attribute

/**
 * Akka Stream attributes that are used when materializing PubSub stream blueprints.
 */
object PubSubAttributes {

  /**
   * gRPC publisher to use for the stream
   */
  def publisher(publisher: GrpcPublisher): Attributes = Attributes(new Publisher(publisher))

  final class Publisher @InternalApi private[PubSubAttributes] (val publisher: GrpcPublisher) extends Attribute

  /**
   * gRPC subscriber to use for the stream
   */
  def subscriber(subscriber: GrpcSubscriber): Attributes = Attributes(new Subscriber(subscriber))

  final class Subscriber @InternalApi private[PubSubAttributes] (val subscriber: GrpcSubscriber) extends Attribute
}
