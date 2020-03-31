/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl

import akka.actor.{
  ActorSystem,
  ClassicActorSystemProvider,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}
import akka.annotation.ApiMayChange
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.AkkaGrpcSettings
import akka.stream.SystemMaterializer
import com.google.pubsub.v1.pubsub.{PublisherClient => ScalaPublisherClient}

/**
 * Holds the gRPC scala publisher client instance.
 */
final class GrpcPublisher private (settings: PubSubSettings, sys: ActorSystem) {

  @ApiMayChange
  final val client =
    ScalaPublisherClient(AkkaGrpcSettings.fromPubSubSettings(settings)(sys))(SystemMaterializer(sys).materializer,
                                                                             sys.dispatcher)

  sys.registerOnTermination(client.close())
}

object GrpcPublisher {
  def apply(settings: PubSubSettings)(implicit sys: ActorSystem): GrpcPublisher =
    new GrpcPublisher(settings, sys)

  def apply()(implicit sys: ActorSystem): GrpcPublisher =
    apply(PubSubSettings(sys))
}

/**
 * An extension that manages a single gRPC scala publisher client per actor system.
 */
final class GrpcPublisherExt private (sys: ExtendedActorSystem) extends Extension {
  implicit val publisher = GrpcPublisher()(sys)
}

object GrpcPublisherExt extends ExtensionId[GrpcPublisherExt] with ExtensionIdProvider {
  override def lookup = GrpcPublisherExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcPublisherExt(system)

  /**
   * Access to extension from the new and classic actors API.
   */
  def apply()(implicit system: ClassicActorSystemProvider): GrpcPublisherExt = super.apply(system)

  /**
   * Access to the extension from the classic actors API.
   */
  override def apply(system: akka.actor.ActorSystem): GrpcPublisherExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcPublisherExt = super.get(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ClassicActorSystemProvider): GrpcPublisherExt = super.get(system)
}
