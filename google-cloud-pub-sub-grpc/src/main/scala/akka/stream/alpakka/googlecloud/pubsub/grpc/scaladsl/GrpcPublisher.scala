/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.ApiMayChange
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.AkkaGrpcSettings
import akka.stream.{ActorMaterializer, Materializer}
import com.google.pubsub.v1.pubsub.{PublisherClient => ScalaPublisherClient}

/**
 * Holds the gRPC scala publisher client instance.
 */
final class GrpcPublisher private (settings: PubSubSettings, sys: ActorSystem, mat: Materializer) {

  @ApiMayChange
  final val client =
    ScalaPublisherClient(AkkaGrpcSettings.fromPubSubSettings(settings)(sys))(mat, sys.dispatcher)

  sys.registerOnTermination(client.close())
}

object GrpcPublisher {
  def apply(settings: PubSubSettings)(implicit sys: ActorSystem, mat: Materializer): GrpcPublisher =
    new GrpcPublisher(settings, sys, mat)

  def apply()(implicit sys: ActorSystem, mat: Materializer): GrpcPublisher =
    apply(PubSubSettings(sys))
}

/**
 * An extension that manages a single gRPC scala publisher client per actor system.
 */
final class GrpcPublisherExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)

  implicit val publisher = GrpcPublisher()(sys, systemMaterializer)
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
