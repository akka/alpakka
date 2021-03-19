/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.javadsl

import akka.actor.{
  ActorSystem,
  ClassicActorSystemProvider,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}
import akka.annotation.ApiMayChange
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.AkkaGrpcSettings
import com.google.pubsub.v1.{PublisherClient => JavaPublisherClient}

/**
 * Holds the gRPC java publisher client instance.
 */
final class GrpcPublisher private (settings: PubSubSettings, googleSettings: GoogleSettings, sys: ActorSystem) {

  @ApiMayChange
  final val client =
    JavaPublisherClient.create(AkkaGrpcSettings.fromPubSubSettings(settings, googleSettings)(sys), sys)

  sys.registerOnTermination(client.close())
}

object GrpcPublisher {

  /**
   * Creates a publisher with the new actors API.
   */
  def create(settings: PubSubSettings, googleSettings: GoogleSettings, sys: ClassicActorSystemProvider): GrpcPublisher =
    create(settings, googleSettings, sys.classicSystem)

  def create(settings: PubSubSettings, googleSettings: GoogleSettings, sys: ActorSystem): GrpcPublisher =
    new GrpcPublisher(settings, googleSettings, sys)

  /**
   * Creates a publisher with the new actors API.
   */
  def create(settings: PubSubSettings, sys: ClassicActorSystemProvider): GrpcPublisher =
    create(settings, sys.classicSystem)

  def create(settings: PubSubSettings, sys: ActorSystem): GrpcPublisher =
    new GrpcPublisher(settings, GoogleSettings(sys), sys)

  /**
   * Creates a publisher with the new actors API.
   */
  def create(sys: ClassicActorSystemProvider): GrpcPublisher =
    create(sys.classicSystem)

  def create(sys: ActorSystem): GrpcPublisher =
    create(PubSubSettings(sys), sys)
}

/**
 * An extension that manages a single gRPC java publisher client per actor system.
 */
final class GrpcPublisherExt private (sys: ExtendedActorSystem) extends Extension {
  implicit val publisher = GrpcPublisher.create(sys)
}

object GrpcPublisherExt extends ExtensionId[GrpcPublisherExt] with ExtensionIdProvider {
  override def lookup = GrpcPublisherExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcPublisherExt(system)

  /**
   * Access to extension.
   */
  @deprecated("use get() instead", since = "2.0.0")
  def apply()(implicit system: ActorSystem): GrpcPublisherExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcPublisherExt = super.get(system)

  /**
   * Java API
   *
   * Access to the extension from the new actors API.
   */
  override def get(system: ClassicActorSystemProvider): GrpcPublisherExt = super.get(system)
}
