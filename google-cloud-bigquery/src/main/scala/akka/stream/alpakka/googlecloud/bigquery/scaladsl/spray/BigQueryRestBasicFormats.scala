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
  implicit val FloatJsonFormat: _root_.spray.json.DefaultJsonProtocol.FloatJsonFormat.type =
    DefaultJsonProtocol.FloatJsonFormat
  implicit val DoubleJsonFormat: _root_.spray.json.DefaultJsonProtocol.DoubleJsonFormat.type =
    DefaultJsonProtocol.DoubleJsonFormat
  implicit val ByteJsonFormat: _root_.spray.json.DefaultJsonProtocol.ByteJsonFormat.type =
    DefaultJsonProtocol.ByteJsonFormat
  implicit val ShortJsonFormat: _root_.spray.json.DefaultJsonProtocol.ShortJsonFormat.type =
    DefaultJsonProtocol.ShortJsonFormat
  implicit val BigDecimalJsonFormat: _root_.spray.json.DefaultJsonProtocol.BigDecimalJsonFormat.type =
    DefaultJsonProtocol.BigDecimalJsonFormat
  implicit val BigIntJsonFormat: _root_.spray.json.DefaultJsonProtocol.BigIntJsonFormat.type =
    DefaultJsonProtocol.BigIntJsonFormat
  implicit val UnitJsonFormat: _root_.spray.json.DefaultJsonProtocol.UnitJsonFormat.type =
    DefaultJsonProtocol.UnitJsonFormat
  implicit val BooleanJsonFormat: _root_.spray.json.DefaultJsonProtocol.BooleanJsonFormat.type =
    DefaultJsonProtocol.BooleanJsonFormat
  implicit val CharJsonFormat: _root_.spray.json.DefaultJsonProtocol.CharJsonFormat.type =
    DefaultJsonProtocol.CharJsonFormat
  implicit val StringJsonFormat: _root_.spray.json.DefaultJsonProtocol.StringJsonFormat.type =
    DefaultJsonProtocol.StringJsonFormat
  implicit val SymbolJsonFormat: _root_.spray.json.DefaultJsonProtocol.SymbolJsonFormat.type =
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
