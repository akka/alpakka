/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

import spray.json.{AdditionalFormats, ProductFormats, StandardFormats}

import scala.reflect.ClassTag

trait ProductSchemas extends ProductSchemasInstances { this: StandardSchemas =>

  protected def extractFieldNames(tag: ClassTag[_]): Array[String] =
    ProductSchemasSupport.extractFieldNames(tag)

}

private object ProductSchemasSupport extends StandardFormats with ProductFormats with AdditionalFormats {
  override def extractFieldNames(tag: ClassTag[_]): Array[String] = super.extractFieldNames(tag)
}
