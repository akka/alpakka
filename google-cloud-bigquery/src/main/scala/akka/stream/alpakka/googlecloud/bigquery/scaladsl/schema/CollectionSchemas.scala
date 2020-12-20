/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import scala.collection.Iterable

trait CollectionSchemas {

  /**
   * Supplies the SchemaWriter for Iterables.
   */
  implicit def iterableSchemaWriter[T](implicit writer: SchemaWriter[T]): SchemaWriter[Iterable[T]] = { (name, mode) =>
    require(mode != Repeated, "A collection cannot be nested inside another collection.")
    writer.write(name, Repeated)
  }

  /**
   * Supplies the SchemaWriter for Arrays.
   */
  implicit def arraySchemaWriter[T](implicit writer: SchemaWriter[T]): SchemaWriter[Array[T]] = { (name, mode) =>
    require(mode != Repeated, "A collection cannot be nested inside another collection.")
    writer.write(name, Repeated)
  }
}
