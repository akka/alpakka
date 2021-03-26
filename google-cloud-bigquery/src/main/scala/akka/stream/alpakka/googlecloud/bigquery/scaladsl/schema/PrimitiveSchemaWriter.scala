/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.{TableFieldSchema, TableFieldSchemaMode, TableFieldSchemaType}

private[schema] final class PrimitiveSchemaWriter[T](`type`: TableFieldSchemaType) extends SchemaWriter[T] {

  override def write(name: String, mode: TableFieldSchemaMode): TableFieldSchema = {
    TableFieldSchema(name, `type`, Some(mode), None)
  }

}
