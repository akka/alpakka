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
import com.google.pubsub.v1.pubsub.{SubscriberClient => ScalaSubscriberClient}

/**
 * Holds the gRPC scala subscriber client instance.
 */
final class GrpcSubscriber private (settings: PubSubSettings, sys: ActorSystem) {

  @ApiMayChange
  final val client =
    ScalaSubscriberClient(AkkaGrpcSettings.fromPubSubSettings(settings)(sys))(SystemMaterializer(sys).materializer,
                                                                              sys.dispatcher)

  sys.registerOnTermination(client.close())
}

object GrpcSubscriber {
  def apply(settings: PubSubSettings)(implicit sys: ActorSystem): GrpcSubscriber =
    new GrpcSubscriber(settings, sys)

  def apply()(implicit sys: ActorSystem): GrpcSubscriber =
    apply(PubSubSettings(sys))
}

/**
 * An extension that manages a single gRPC scala subscriber client per actor system.
 */
final class GrpcSubscriberExt private (sys: ExtendedActorSystem) extends Extension {
  implicit val subscriber = GrpcSubscriber()(sys)
}

object GrpcSubscriberExt extends ExtensionId[GrpcSubscriberExt] with ExtensionIdProvider {
  override def lookup = GrpcSubscriberExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcSubscriberExt(system)

  /**
   * Access to extension.
   */
  def apply()(implicit system: ActorSystem): GrpcSubscriberExt = super.apply(system)

  /**
   * Access to the extension from the new actors API.
   */
  def apply(system: ClassicActorSystemProvider): GrpcSubscriberExt = super.apply(system.classicSystem)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcSubscriberExt = super.get(system)
}
