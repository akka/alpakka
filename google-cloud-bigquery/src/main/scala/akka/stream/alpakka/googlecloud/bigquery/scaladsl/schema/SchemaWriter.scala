/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.{TableFieldSchema, TableFieldSchemaMode, TableSchema}

import scala.annotation.implicitNotFound

/**
 * Provides a [[akka.stream.alpakka.googlecloud.bigquery.model.TableFieldSchema]] for type [[T]].
 */
@implicitNotFound(msg = "Cannot find SchemaWriter type class for ${T}")
trait SchemaWriter[-T] {

  def write(name: String, mode: TableFieldSchemaMode): TableFieldSchema

}

object SchemaWriter {

  def apply[T](implicit writer: SchemaWriter[T]): SchemaWriter[T] = writer

}

/**
 * Provides a [[akka.stream.alpakka.googlecloud.bigquery.model.TableSchema]] for type [[T]].
 */
@implicitNotFound(msg = "Cannot find TableSchemaWriter type class for ${T}")
trait TableSchemaWriter[-T] extends SchemaWriter[T] {

  def write: TableSchema

}

object TableSchemaWriter {

  def apply[T](implicit writer: TableSchemaWriter[T]): TableSchemaWriter[T] = writer

}
