/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb

import akka.NotUsed
import akka.annotation.ApiMayChange

/**
 * API may change.
 */
@ApiMayChange
object InfluxDbWriteMessage {
  // Apply method to use when not using passThrough
  def apply[T](point: T): InfluxDbWriteMessage[T, NotUsed] =
    new InfluxDbWriteMessage(point = point, passThrough = NotUsed)

  // Java-api - without passThrough
  def create[T](point: T): InfluxDbWriteMessage[T, NotUsed] =
    new InfluxDbWriteMessage(point, NotUsed)

  // Java-api - with passThrough
  def create[T, C](point: T, passThrough: C) =
    new InfluxDbWriteMessage(point, passThrough)
}

/**
 * API may change.
 */
@ApiMayChange
final class InfluxDbWriteMessage[T, C] private (val point: T,
                                                val passThrough: C,
                                                val databaseName: Option[String] = None,
                                                val retentionPolicy: Option[String] = None) {

  def withPoint(point: T): InfluxDbWriteMessage[T, C] =
    copy(point = point)

  def withPassThrough[PT2](passThrough: PT2): InfluxDbWriteMessage[T, PT2] =
    new InfluxDbWriteMessage[T, PT2](
      point = point,
      passThrough = passThrough,
      databaseName = databaseName,
      retentionPolicy = retentionPolicy
    )

  def withDatabaseName(databaseName: String): InfluxDbWriteMessage[T, C] =
    copy(databaseName = Some(databaseName))

  def withRetentionPolicy(retentionPolicy: String): InfluxDbWriteMessage[T, C] =
    copy(retentionPolicy = Some(retentionPolicy))

  private def copy(
      point: T = point,
      passThrough: C = passThrough,
      databaseName: Option[String] = databaseName,
      retentionPolicy: Option[String] = retentionPolicy
  ): InfluxDbWriteMessage[T, C] =
    new InfluxDbWriteMessage(point = point,
                             passThrough = passThrough,
                             databaseName = databaseName,
                             retentionPolicy = retentionPolicy)

  override def toString: String =
    "InfluxDbWriteMessage(" +
    s"point=$point," +
    s"passThrough=$passThrough," +
    s"databaseName=$databaseName," +
    s"retentionPolicy=$retentionPolicy" +
    ")"
}

/**
 * API may change.
 */
@ApiMayChange
final case class InfluxDbWriteResult[T, C](writeMessage: InfluxDbWriteMessage[T, C], error: Option[String])
