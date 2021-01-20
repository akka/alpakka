/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{
  RecordType,
  TableFieldSchema,
  TableFieldSchemaMode,
  TableSchema
}
import spray.json.{AdditionalFormats, ProductFormats, StandardFormats}

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

trait ProductSchemas extends ProductSchemasInstances { this: StandardSchemas =>

  protected def extractFieldNames(tag: ClassTag[_]): Array[String] =
    ProductSchemasSupport.extractFieldNames(tag)

}

final class ProductSchemaWriter[T <: Product](fieldSchemas: Seq[TableFieldSchema]) extends TableSchemaWriter[T] {

  override def write: TableSchema = TableSchema(fieldSchemas)

  override def write(name: String, mode: TableFieldSchemaMode): TableFieldSchema =
    TableFieldSchema(name, RecordType, Some(mode), Some(fieldSchemas))

}

private object ProductSchemasSupport extends StandardFormats with ProductFormats with AdditionalFormats {
  override def extractFieldNames(tag: ClassTag[_]): Array[String] = super.extractFieldNames(tag)
}
