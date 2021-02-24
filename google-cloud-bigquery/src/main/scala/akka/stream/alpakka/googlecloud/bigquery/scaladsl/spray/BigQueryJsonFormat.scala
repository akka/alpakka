/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{JsonFormat, JsonReader, JsonWriter, RootJsonWriter}

import scala.annotation.implicitNotFound

/**
 * A special JsonReader capable of reading a cell from a BigQuery table.
 */
@implicitNotFound(msg = "Cannot find BigQueryJsonReader or BigQueryJsonFormat type class for ${T}")
trait BigQueryJsonReader[T] extends JsonReader[T]

/**
 * A special JsonWriter capable of writing a cell to a BigQuery table.
 */
@implicitNotFound(msg = "Cannot find BigQueryJsonWriter or BigQueryJsonFormat type class for ${T}")
trait BigQueryJsonWriter[T] extends JsonWriter[T]

/**
 * A special JsonFormat signaling that the format reads and writes cells of BigQuery tables.
 */
trait BigQueryJsonFormat[T] extends JsonFormat[T] with BigQueryJsonReader[T] with BigQueryJsonWriter[T]

/**
 * A special JsonReader capable of reading a row from a BigQuery table.
 */
@implicitNotFound(msg = "Cannot find BigQueryRootJsonReader or BigQueryRootJsonFormat type class for ${T}")
trait BigQueryRootJsonReader[T] extends BigQueryJsonReader[T]

/**
 * A special JsonWriter capable of writing a row to a BigQuery table.
 */
@implicitNotFound(msg = "Cannot find BigQueryRootJsonWriter or BigQueryRootJsonFormat type class for ${T}")
trait BigQueryRootJsonWriter[T] extends BigQueryJsonWriter[T] with RootJsonWriter[T]

/**
 * A special JsonFormat signaling that the format produces a row of BigQuery table.
 */
trait BigQueryRootJsonFormat[T]
    extends BigQueryJsonFormat[T]
    with BigQueryRootJsonReader[T]
    with BigQueryRootJsonWriter[T]
