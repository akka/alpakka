/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json
import spray.json.{deserializationError, DefaultJsonProtocol, JsNumber, JsValue, JsonFormat}

import scala.concurrent.duration.{DurationLong, FiniteDuration}

/**
 * Provides the JsonFormats for the BigQuery REST API's representation of the most important Scala types.
 */
trait BigQueryRestBasicFormats {

  implicit val IntJsonFormat: json.DefaultJsonProtocol.IntJsonFormat.type = DefaultJsonProtocol.IntJsonFormat
  implicit val FloatJsonFormat: json.DefaultJsonProtocol.FloatJsonFormat.type =
    DefaultJsonProtocol.FloatJsonFormat
  implicit val DoubleJsonFormat: json.DefaultJsonProtocol.DoubleJsonFormat.type =
    DefaultJsonProtocol.DoubleJsonFormat
  implicit val ByteJsonFormat: json.DefaultJsonProtocol.ByteJsonFormat.type =
    DefaultJsonProtocol.ByteJsonFormat
  implicit val ShortJsonFormat: json.DefaultJsonProtocol.ShortJsonFormat.type =
    DefaultJsonProtocol.ShortJsonFormat
  implicit val BigDecimalJsonFormat: json.DefaultJsonProtocol.BigDecimalJsonFormat.type =
    DefaultJsonProtocol.BigDecimalJsonFormat
  implicit val BigIntJsonFormat: json.DefaultJsonProtocol.BigIntJsonFormat.type =
    DefaultJsonProtocol.BigIntJsonFormat
  implicit val UnitJsonFormat: json.DefaultJsonProtocol.UnitJsonFormat.type =
    DefaultJsonProtocol.UnitJsonFormat
  implicit val BooleanJsonFormat: json.DefaultJsonProtocol.BooleanJsonFormat.type =
    DefaultJsonProtocol.BooleanJsonFormat
  implicit val CharJsonFormat: json.DefaultJsonProtocol.CharJsonFormat.type =
    DefaultJsonProtocol.CharJsonFormat
  implicit val StringJsonFormat: json.DefaultJsonProtocol.StringJsonFormat.type =
    DefaultJsonProtocol.StringJsonFormat
  implicit val SymbolJsonFormat: json.DefaultJsonProtocol.SymbolJsonFormat.type =
    DefaultJsonProtocol.SymbolJsonFormat

  implicit object BigQueryLongJsonFormat extends JsonFormat[Long] {
    def write(x: Long) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) if x.isValidLong => x.longValue
      case BigQueryNumber(x) if x.isValidLong => x.longValue
      case x => deserializationError("Expected Long as JsNumber or JsString, but got " + x)
    }
  }

  implicit object BigQueryFiniteDurationJsonFormat extends JsonFormat[FiniteDuration] {
    override def write(x: FiniteDuration): JsValue = JsNumber(x.toMillis)
    override def read(value: JsValue): FiniteDuration = value match {
      case JsNumber(x) if x.isValidLong => x.longValue.millis
      case BigQueryNumber(x) if x.isValidLong => x.longValue.millis
      case x => deserializationError("Expected FiniteDuration as JsNumber or JsString, but got " + x)
    }
  }
}
