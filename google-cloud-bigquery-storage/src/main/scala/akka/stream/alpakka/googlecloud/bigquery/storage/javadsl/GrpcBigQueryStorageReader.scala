/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.ApiMayChange
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.AkkaGrpcSettings
import akka.stream.{ActorMaterializer, Materializer}
import com.google.cloud.bigquery.storage.v1.{BigQueryReadClient => JavaBigQueryReadClient}

/**
 * Holds the gRPC java reader client instance.
 */
final class GrpcBigQueryStorageReader private (settings: BigQueryStorageSettings, sys: ActorSystem, mat: Materializer) {

  @ApiMayChange
  final val client =
    JavaBigQueryReadClient.create(AkkaGrpcSettings.fromBigQuerySettings(settings)(sys), mat, sys.dispatcher)

  sys.registerOnTermination(client.close())
}

object GrpcBigQueryStorageReader {
  def create(settings: BigQueryStorageSettings, sys: ActorSystem, mat: Materializer): GrpcBigQueryStorageReader =
    new GrpcBigQueryStorageReader(settings, sys, mat)

  def create(sys: ActorSystem, mat: Materializer): GrpcBigQueryStorageReader =
    create(BigQueryStorageSettings(sys), sys, mat)
}

/**
 * An extension that manages a single gRPC java reader client per actor system.
 */
final class GrpcBigQueryStorageReaderExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)

  implicit val reader = GrpcBigQueryStorageReader.create(sys, systemMaterializer)
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
