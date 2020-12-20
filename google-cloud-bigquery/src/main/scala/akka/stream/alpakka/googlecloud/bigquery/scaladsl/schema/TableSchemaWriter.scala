/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.TableSchema

trait TableSchemaWriter[-T] extends SchemaWriter[T] {

  def write: TableSchema

}

object TableSchemaWriter {

  def apply[T](implicit writer: TableSchemaWriter[T]): TableSchemaWriter[T] = writer

}
