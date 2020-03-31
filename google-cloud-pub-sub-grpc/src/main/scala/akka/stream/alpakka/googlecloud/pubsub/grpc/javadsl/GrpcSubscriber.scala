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
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.AkkaGrpcSettings
import akka.stream.SystemMaterializer
import com.google.pubsub.v1.{SubscriberClient => JavaSubscriberClient}

/**
 * Holds the gRPC java subscriber client instance.
 */
final class GrpcSubscriber private (settings: PubSubSettings, sys: ActorSystem) {

  @ApiMayChange
  final val client =
    JavaSubscriberClient.create(AkkaGrpcSettings.fromPubSubSettings(settings)(sys),
                                SystemMaterializer(sys).materializer,
                                sys.dispatcher)

  sys.registerOnTermination(client.close())
}

object GrpcSubscriber {

  /**
   * Creates a publisher with the new actors API.
   */
  def create(settings: PubSubSettings, sys: ClassicActorSystemProvider): GrpcSubscriber =
    create(settings, sys.classicSystem)

  def create(settings: PubSubSettings, sys: ActorSystem): GrpcSubscriber =
    new GrpcSubscriber(settings, sys)

  /**
   * Creates a publisher with the new actors API.
   */
  def create(sys: ClassicActorSystemProvider): GrpcSubscriber = create(sys.classicSystem)

  def create(sys: ActorSystem): GrpcSubscriber =
    create(PubSubSettings(sys), sys)
}

/**
 * An extension that manages a single gRPC java subscriber client per actor system.
 */
final class GrpcSubscriberExt private (sys: ExtendedActorSystem) extends Extension {
  implicit val subscriber = GrpcSubscriber.create(sys)
}

object GrpcSubscriberExt extends ExtensionId[GrpcSubscriberExt] with ExtensionIdProvider {
  override def lookup = GrpcSubscriberExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcSubscriberExt(system)

  /**
   * Access to extension.
   */
  @deprecated("use get() instead", since = "2.0.0")
  def apply()(implicit system: ActorSystem): GrpcSubscriberExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcSubscriberExt = super.get(system)

  /**
   * Java API
   *
   * Access to the extension from the new actors API.
   */
  override def get(system: ClassicActorSystemProvider): GrpcSubscriberExt = super.get(system.classicSystem)
}
