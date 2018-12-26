/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.ApiMayChange
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.AkkaGrpcSettings
import akka.stream.{ActorMaterializer, Materializer}
import com.google.pubsub.v1.pubsub.{SubscriberClient => ScalaSubscriberClient}

/**
 * Holds the gRPC scala subscriber client instance.
 */
final class GrpcSubscriber(sys: ActorSystem, mat: Materializer) {
  private final val pubSubConfig = PubSubSettings(sys)

  @ApiMayChange
  final val client =
    ScalaSubscriberClient(AkkaGrpcSettings.fromPubSubConfig(pubSubConfig)(sys))(mat, sys.dispatcher)

  sys.registerOnTermination(client.close())
}

/**
 * An extension that manages a single gRPC scala subscriber client per actor system.
 */
final class GrpcSubscriberExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)

  implicit val subscriber = new GrpcSubscriber(sys, systemMaterializer)
}

object GrpcSubscriberExt extends ExtensionId[GrpcSubscriberExt] with ExtensionIdProvider {
  override def lookup = GrpcSubscriberExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcSubscriberExt(system)

  /**
   * Access to extension.
   */
  def apply()(implicit system: ActorSystem): GrpcSubscriberExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcSubscriberExt = super.get(system)
}
