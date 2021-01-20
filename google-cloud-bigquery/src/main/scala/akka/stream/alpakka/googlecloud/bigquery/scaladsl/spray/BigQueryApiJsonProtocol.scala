/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{
  deserializationError,
  AdditionalFormats,
  CollectionFormats,
  DefaultJsonProtocol,
  JsNumber,
  JsString,
  JsValue,
  JsonFormat,
  ProductFormats,
  StandardFormats
}

import scala.util.Try

object BigQueryApiJsonProtocol extends BigQueryApiJsonProtocol

trait BigQueryApiJsonProtocol
    extends StandardFormats
    with CollectionFormats
    with ProductFormats
    with AdditionalFormats {

  implicit val IntJsonFormat = DefaultJsonProtocol.IntJsonFormat
  implicit val FloatJsonFormat = DefaultJsonProtocol.FloatJsonFormat
  implicit val DoubleJsonFormat = DefaultJsonProtocol.DoubleJsonFormat
  implicit val ByteJsonFormat = DefaultJsonProtocol.ByteJsonFormat
  implicit val ShortJsonFormat = DefaultJsonProtocol.ShortJsonFormat
  implicit val BigDecimalJsonFormat = DefaultJsonProtocol.BigDecimalJsonFormat
  implicit val BigIntJsonFormat = DefaultJsonProtocol.BigIntJsonFormat
  implicit val UnitJsonFormat = DefaultJsonProtocol.UnitJsonFormat
  implicit val BooleanJsonFormat = DefaultJsonProtocol.BooleanJsonFormat
  implicit val CharJsonFormat = DefaultJsonProtocol.CharJsonFormat
  implicit val StringJsonFormat = DefaultJsonProtocol.StringJsonFormat
  implicit val SymbolJsonFormat = DefaultJsonProtocol.SymbolJsonFormat

  implicit object BigQueryLongJsonFormat extends JsonFormat[Long] {
    def write(x: Long) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) if x.isValidLong => x.longValue
      case JsString(BigQueryLong(x)) => x
      case x => deserializationError("Expected Long as JsNumber or JsString, but got " + x)
    }
  }

  private object BigQueryLong {
    def unapply(long: String): Option[Long] = Try(long.toLong).toOption
  }

}
