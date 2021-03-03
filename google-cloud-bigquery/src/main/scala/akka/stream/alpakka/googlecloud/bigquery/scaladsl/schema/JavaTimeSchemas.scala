/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{
  DateTimeType,
  DateType,
  TimeType,
  TimestampType
}

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

/**
 * Provides BigQuery schemas for [[java.time]] classes.
 */
trait JavaTimeSchemas {
  implicit val localDateSchemaWriter: SchemaWriter[LocalDate] = new PrimitiveSchemaWriter(DateType)
  implicit val localTimeSchemaWriter: SchemaWriter[LocalTime] = new PrimitiveSchemaWriter(TimeType)
  implicit val localDateTimeSchemaWriter: SchemaWriter[LocalDateTime] = new PrimitiveSchemaWriter(DateTimeType)
  implicit val instantSchemaWriter: SchemaWriter[Instant] = new PrimitiveSchemaWriter[Instant](TimestampType)
}
