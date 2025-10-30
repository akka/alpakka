/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableFieldSchemaType.Record
import akka.stream.alpakka.googlecloud.bigquery.model.{TableFieldSchema, TableFieldSchemaMode, TableSchema}
import spray.json.{AdditionalFormats, ProductFormats, StandardFormats}

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

/**
 * Provides the helpers for constructing custom SchemaWriter implementations for types implementing the Product trait
 * (especially case classes)
 */
trait ProductSchemas extends ProductSchemasInstances { this: StandardSchemas =>

  protected def extractFieldNames(tag: ClassTag[_]): Array[String] =
    ProductSchemasSupport.extractFieldNames(tag)

}

private[schema] final class ProductSchemaWriter[T <: Product](fieldSchemas: Seq[TableFieldSchema])
    extends TableSchemaWriter[T] {

  override def write: TableSchema = TableSchema(fieldSchemas)

  override def write(name: String, mode: TableFieldSchemaMode): TableFieldSchema =
    TableFieldSchema(name, Record, Some(mode), Some(fieldSchemas))

}

private object ProductSchemasSupport extends StandardFormats with ProductFormats with AdditionalFormats {
  override def extractFieldNames(tag: ClassTag[_]): Array[String] = super.extractFieldNames(tag)
}
