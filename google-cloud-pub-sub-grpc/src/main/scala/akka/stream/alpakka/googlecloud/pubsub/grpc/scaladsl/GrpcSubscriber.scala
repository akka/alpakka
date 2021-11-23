/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
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
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.AkkaGrpcSettings
import com.google.pubsub.v1.pubsub.{SubscriberClient => ScalaSubscriberClient}

/**
 * Holds the gRPC scala subscriber client instance.
 */
final class GrpcSubscriber private (settings: PubSubSettings, googleSettings: GoogleSettings, sys: ActorSystem) {

  @ApiMayChange
  final val client =
    ScalaSubscriberClient(AkkaGrpcSettings.fromPubSubSettings(settings, googleSettings)(sys))(sys)

  sys.registerOnTermination(client.close())
}

object GrpcSubscriber {
  def apply(settings: PubSubSettings,
            googleSettings: GoogleSettings)(implicit sys: ClassicActorSystemProvider): GrpcSubscriber =
    new GrpcSubscriber(settings, googleSettings, sys.classicSystem)

  def apply(settings: PubSubSettings, googleSettings: GoogleSettings, sys: ActorSystem): GrpcSubscriber =
    new GrpcSubscriber(settings, googleSettings, sys)

  def apply(settings: PubSubSettings)(implicit sys: ClassicActorSystemProvider): GrpcSubscriber =
    new GrpcSubscriber(settings, GoogleSettings(), sys.classicSystem)

  def apply(settings: PubSubSettings, sys: ActorSystem): GrpcSubscriber =
    new GrpcSubscriber(settings, GoogleSettings(sys), sys)

  def apply()(implicit sys: ClassicActorSystemProvider): GrpcSubscriber =
    apply(PubSubSettings(sys))

  def apply(sys: ActorSystem): GrpcSubscriber =
    apply(PubSubSettings(sys), sys)
}

/**
 * An extension that manages a single gRPC scala subscriber client per actor system.
 */
final class GrpcSubscriberExt private (sys: ExtendedActorSystem) extends Extension {
  implicit val subscriber = GrpcSubscriber(sys: ActorSystem)
}

object GrpcSubscriberExt extends ExtensionId[GrpcSubscriberExt] with ExtensionIdProvider {
  override def lookup = GrpcSubscriberExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcSubscriberExt(system)

  /**
   * Access to extension from the new and classic actors API.
   */
  def apply()(implicit system: ClassicActorSystemProvider): GrpcSubscriberExt = super.apply(system)

  /**
   * Access to the extension from the classic actors API.
   */
  override def apply(system: ActorSystem): GrpcSubscriberExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcSubscriberExt = super.get(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ClassicActorSystemProvider): GrpcSubscriberExt = super.get(system)
}
