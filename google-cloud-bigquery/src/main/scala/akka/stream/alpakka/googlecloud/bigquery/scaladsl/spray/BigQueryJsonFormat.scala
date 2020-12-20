/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{JsonFormat, JsonReader, JsonWriter}

import scala.annotation.implicitNotFound

/**
 * A special JsonReader capable of reading a BigQuery-encoded JSON object.
 */
@implicitNotFound(msg = "Cannot find BigQueryJsonReader or BigQueryJsonFormat type class for ${T}")
trait BigQueryJsonReader[T] extends JsonReader[T]

/**
 * A special JsonWriter capable of writing a BigQuery-encoded JSON object.
 */
@implicitNotFound(msg = "Cannot find BigQueryJsonWriter or BigQueryJsonFormat type class for ${T}")
trait BigQueryJsonWriter[T] extends JsonWriter[T]

/**
 * A special JsonFormat signaling that the format reads and writes BigQuery-encoded JSON objects.
 */
trait BigQueryJsonFormat[T] extends JsonFormat[T] with BigQueryJsonReader[T] with BigQueryJsonWriter[T]
