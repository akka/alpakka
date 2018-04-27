/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.javadsl

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.ApiMayChange
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.AkkaGrpcSettings
import akka.stream.{ActorMaterializer, Materializer}
import com.google.pubsub.v1.{PublisherClient => JavaPublisherClient}

/**
 * Holds the gRPC java publisher client instance.
 */
final class GrpcPublisher(sys: ActorSystem, mat: Materializer) {
  private final val pubSubConfig = PubSubSettings(sys)

  @ApiMayChange
  final val client =
    JavaPublisherClient.create(AkkaGrpcSettings.fromPubSubConfig(pubSubConfig)(sys), mat, sys.dispatcher)

  sys.registerOnTermination(client.close())
}

/**
 * An extension that manages a single gRPC java publisher client per actor system.
 */
final class GrpcPublisherExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)

  implicit val publisher = new GrpcPublisher(sys, systemMaterializer)
}

object GrpcPublisherExt extends ExtensionId[GrpcPublisherExt] with ExtensionIdProvider {
  override def lookup = GrpcPublisherExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcPublisherExt(system)

  /**
   * Access to extension.
   */
  def apply()(implicit system: ActorSystem): GrpcPublisherExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcPublisherExt = super.get(system)
}
