/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{deserializationError, DefaultJsonProtocol, JsNumber, JsValue, JsonFormat}

import scala.concurrent.duration.{DurationLong, FiniteDuration}

/**
 * Provides the JsonFormats for the BigQuery REST API's representation of the most important Scala types.
 */
trait BigQueryRestBasicFormats {

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
