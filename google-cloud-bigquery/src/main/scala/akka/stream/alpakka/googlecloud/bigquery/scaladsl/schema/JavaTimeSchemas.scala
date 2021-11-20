/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableFieldSchemaType.{Date, DateTime, Time, Timestamp}

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

/**
 * Provides BigQuery schemas for [[java.time]] classes.
 */
trait JavaTimeSchemas {
  implicit val localDateSchemaWriter: SchemaWriter[LocalDate] = new PrimitiveSchemaWriter(Date)
  implicit val localTimeSchemaWriter: SchemaWriter[LocalTime] = new PrimitiveSchemaWriter(Time)
  implicit val localDateTimeSchemaWriter: SchemaWriter[LocalDateTime] = new PrimitiveSchemaWriter(DateTime)
  implicit val instantSchemaWriter: SchemaWriter[Instant] = new PrimitiveSchemaWriter[Instant](Timestamp)
}
