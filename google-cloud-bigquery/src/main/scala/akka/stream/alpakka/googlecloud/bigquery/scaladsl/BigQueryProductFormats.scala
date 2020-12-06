/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import spray.json.{
  deserializationError,
  DeserializationException,
  JsArray,
  JsValue,
  JsonReader,
  ProductFormats,
  StandardFormats
}

trait BigQueryProductFormats extends BigQueryProductFormatsInstances { this: ProductFormats with StandardFormats =>

  protected def fromBigQueryField[T](value: JsValue, fieldName: String, index: Int)(implicit reader: JsonReader[T]) =
    value match {
      case x: JsArray =>
        try reader.read(x.elements(index).asJsObject.fields("v"))
        catch {
          case e: IndexOutOfBoundsException =>
            deserializationError("Object is missing required member '" + fieldName + "'", e, fieldName :: Nil)
          case DeserializationException(msg, cause, fieldNames) =>
            deserializationError(msg, cause, fieldName :: fieldNames)
        }
      case _ => deserializationError("Object expected in field '" + fieldName + "'", fieldNames = fieldName :: Nil)
    }

}
