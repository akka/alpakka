/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.ApiMayChange
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.AkkaGrpcSettings
import com.google.cloud.bigquery.storage.v1.storage.{BigQueryReadClient => ScalaBigQueryReadClient}

/**
 * Holds the gRPC scala reader client instance.
 */
final class GrpcBigQueryStorageReader private (settings: BigQueryStorageSettings, sys: ActorSystem, mat: Materializer) {

  @ApiMayChange
  final val client = ScalaBigQueryReadClient(AkkaGrpcSettings.fromBigQuerySettings(settings)(sys))(sys)

  sys.registerOnTermination(client.close())
}

object GrpcBigQueryStorageReader {

  def apply(settings: BigQueryStorageSettings)(implicit sys: ActorSystem,
                                               mat: Materializer): GrpcBigQueryStorageReader =
    new GrpcBigQueryStorageReader(settings, sys, mat)

  def apply()(implicit sys: ActorSystem, mat: Materializer): GrpcBigQueryStorageReader =
    apply(BigQueryStorageSettings(sys))
}

/**
 * An extension that manages a single gRPC scala reader client per actor system.
 */
final class GrpcBigQueryStorageReaderExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)

  implicit val reader = GrpcBigQueryStorageReader()(sys, systemMaterializer)
}

object GrpcBigQueryStorageReaderExt extends ExtensionId[GrpcBigQueryStorageReaderExt] with ExtensionIdProvider {
  override def lookup = GrpcBigQueryStorageReaderExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcBigQueryStorageReaderExt(system)

  /**
   * Access to extension.
   */
  def apply()(implicit system: ActorSystem): GrpcBigQueryStorageReaderExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcBigQueryStorageReaderExt = super.get(system)
}
