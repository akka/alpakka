/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb

import akka.NotUsed

object InfluxDBWriteMessage {
  // Apply method to use when not using passThrough
  def apply[T](point: T): InfluxDBWriteMessage[T, NotUsed] =
    InfluxDBWriteMessage(point = point, passThrough = NotUsed)

  // Java-api - without passThrough
  def create[T](point: T): InfluxDBWriteMessage[T, NotUsed] =
    InfluxDBWriteMessage(point, NotUsed)

  // Java-api - with passThrough
  def create[T, C](point: T, passThrough: C) =
    InfluxDBWriteMessage(point, passThrough)
}

final case class InfluxDBWriteMessage[T, C](point: T,
                                            passThrough: C,
                                            databaseName: Option[String] = None,
                                            retentionPolicy: Option[String] = None) {

  def withPoint(point: T): InfluxDBWriteMessage[T, C] =
    copy(point = point)

  def withDatabaseName(databaseName: String): InfluxDBWriteMessage[T, C] =
    copy(databaseName = Some(databaseName))

  def withRetentionPolicy(retentionPolicy: String): InfluxDBWriteMessage[T, C] =
    copy(retentionPolicy = Some(retentionPolicy))

  private def copy(
      point: T = point,
      passThrough: C = passThrough,
      databaseName: Option[String] = databaseName,
      retentionPolicy: Option[String] = retentionPolicy
  ): InfluxDBWriteMessage[T, C] =
    new InfluxDBWriteMessage(point = point,
                             passThrough = passThrough,
                             databaseName = databaseName,
                             retentionPolicy = retentionPolicy)
}

final case class InfluxDBWriteResult[T, C](writeMessage: InfluxDBWriteMessage[T, C], error: Option[String])
