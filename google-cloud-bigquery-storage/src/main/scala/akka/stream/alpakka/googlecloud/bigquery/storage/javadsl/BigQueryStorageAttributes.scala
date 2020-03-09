/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl

import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.Attributes.Attribute

/**
 * Akka Stream attributes that are used when materializing BigQuery Storage stream blueprints.
 */
object BigQueryStorageAttributes {

  /**
   * gRPC reader client to use for the stream
   */
  def reader(client: GrpcBigQueryStorageReader): Attributes = Attributes(new BigQueryStorageReader(client))

  final class BigQueryStorageReader @InternalApi private[BigQueryStorageAttributes] (
      val client: GrpcBigQueryStorageReader
  ) extends Attribute
}
