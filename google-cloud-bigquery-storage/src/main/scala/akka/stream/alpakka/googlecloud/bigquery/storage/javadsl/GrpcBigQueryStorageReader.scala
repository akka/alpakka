/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl

import akka.actor.{ClassicActorSystemProvider, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.ApiMayChange
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.AkkaGrpcSettings
import com.google.cloud.bigquery.storage.v1.storage.BigQueryReadClient

/**
 * Holds the gRPC java reader client instance.
 */
final class GrpcBigQueryStorageReader private (settings: BigQueryStorageSettings, sys: ClassicActorSystemProvider) {

  @ApiMayChange
  final val client = BigQueryReadClient(AkkaGrpcSettings.fromBigQuerySettings(settings)(sys))(sys)

  sys.classicSystem.registerOnTermination(client.close())
}

object GrpcBigQueryStorageReader {
  def create(settings: BigQueryStorageSettings, sys: ClassicActorSystemProvider): GrpcBigQueryStorageReader =
    new GrpcBigQueryStorageReader(settings, sys)

  def create(sys: ClassicActorSystemProvider): GrpcBigQueryStorageReader =
    create(BigQueryStorageSettings(sys), sys)
}

/**
 * An extension that manages a single gRPC java reader client per actor system.
 */
final class GrpcBigQueryStorageReaderExt private (sys: ExtendedActorSystem) extends Extension {

  implicit val reader = GrpcBigQueryStorageReader.create(sys)
}

object GrpcBigQueryStorageReaderExt extends ExtensionId[GrpcBigQueryStorageReaderExt] with ExtensionIdProvider {
  override def lookup = GrpcBigQueryStorageReaderExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcBigQueryStorageReaderExt(system)

  /**
   * Access to extension.
   */
  def apply()(implicit system: ClassicActorSystemProvider): GrpcBigQueryStorageReaderExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ClassicActorSystemProvider): GrpcBigQueryStorageReaderExt = super.get(system)
}
