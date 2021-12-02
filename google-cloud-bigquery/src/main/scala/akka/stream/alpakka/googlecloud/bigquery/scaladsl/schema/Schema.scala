/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableSchema

object Schema {

  /**
   * Materialize an implicit [[TableSchema]] for `T`
   */
  def apply[T](implicit writer: TableSchemaWriter[T]): TableSchema =
    writer.write
}
