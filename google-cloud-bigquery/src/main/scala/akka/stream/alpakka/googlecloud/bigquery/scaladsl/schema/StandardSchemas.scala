/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

trait StandardSchemas {

  implicit def optionSchemaWriter[T](implicit writer: SchemaWriter[T]): SchemaWriter[Option[T]] = { (name, mode) =>
    require(mode == Required, "An option cannot be nested inside another option or a collection.")
    writer.write(name, Nullable)
  }

}
