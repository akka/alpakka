/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{
  NullableMode,
  RepeatedMode,
  RequiredMode,
  TableFieldSchema,
  TableSchema
}

trait SchemaWriter[-T] {

  def write(name: String, mode: FieldMode): TableFieldSchema

}

object SchemaWriter {

  def apply[T](implicit writer: SchemaWriter[T]): SchemaWriter[T] = writer

}

sealed abstract class FieldMode
case object Nullable extends FieldMode {
  override def toString: String = NullableMode
}
case object Required extends FieldMode {
  override def toString: String = RequiredMode
}
case object Repeated extends FieldMode {
  override def toString: String = RepeatedMode
}

trait TableSchemaWriter[-T] extends SchemaWriter[T] {

  def write: TableSchema

}

object TableSchemaWriter {

  def apply[T](implicit writer: TableSchemaWriter[T]): TableSchemaWriter[T] = writer

}
