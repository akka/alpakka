/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import akka.util.ByteString
import spray.json.{deserializationError, JsBoolean, JsFalse, JsNumber, JsString, JsTrue, JsValue}

/**
 * Provides the BigQueryJsonFormats for BigQuery table cells of the most important Scala types.
 */
trait BigQueryBasicFormats {

  implicit object IntJsonFormat extends BigQueryJsonFormat[Int] {
    def write(x: Int) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) if x.isValidInt => x.intValue
      case BigQueryNumber(x) if x.isValidInt => x.intValue
      case x => deserializationError("Expected Int as JsNumber or JsString, but got " + x)
    }
  }

  implicit object LongJsonFormat extends BigQueryJsonFormat[Long] {
    def write(x: Long) =
      if (-9007199254740991L <= x & x <= 9007199254740991L)
        JsNumber(x)
      else
        JsString(x.toString)
    def read(value: JsValue) = value match {
      case JsNumber(x) if x.isValidLong => x.longValue
      case BigQueryNumber(x) if x.isValidLong => x.longValue
      case x => deserializationError("Expected Long as JsNumber or JsString, but got " + x)
    }
  }

  implicit object FloatJsonFormat extends BigQueryJsonFormat[Float] {
    def write(x: Float) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.floatValue
      case BigQueryNumber(x) => x.floatValue
      case x => deserializationError("Expected Float as JsNumber or JsString, but got " + x)
    }
  }

  implicit object DoubleJsonFormat extends BigQueryJsonFormat[Double] {
    def write(x: Double) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.doubleValue
      case BigQueryNumber(x) => x.doubleValue
      case x => deserializationError("Expected Double as JsNumber or JsString, but got " + x)
    }
  }

  implicit object ByteJsonFormat extends BigQueryJsonFormat[Byte] {
    def write(x: Byte) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) if x.isValidByte => x.byteValue
      case BigQueryNumber(x) if x.isValidByte => x.byteValue
      case x => deserializationError("Expected Byte as JsNumber or JsString, but got " + x)
    }
  }

  implicit object ShortJsonFormat extends BigQueryJsonFormat[Short] {
    def write(x: Short) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) if x.isValidShort => x.shortValue
      case BigQueryNumber(x) if x.isValidShort => x.shortValue
      case x => deserializationError("Expected Short as JsNumber or JsString, but got " + x)
    }
  }

  implicit object BigDecimalJsonFormat extends BigQueryJsonFormat[BigDecimal] {
    def write(x: BigDecimal) = {
      require(x ne null)
      JsString(x.toString)
    }
    def read(value: JsValue) = value match {
      case JsNumber(x) => x
      case BigQueryNumber(x) => x
      case x => deserializationError("Expected BigDecimal as JsNumber or JsString, but got " + x)
    }
  }

  implicit object BigIntJsonFormat extends BigQueryJsonFormat[BigInt] {
    def write(x: BigInt) = {
      require(x ne null)
      JsString(x.toString)
    }
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.toBigInt
      case BigQueryNumber(x) => x.toBigInt
      case x => deserializationError("Expected BigInt as JsNumber or JsString, but got " + x)
    }
  }

  implicit object UnitJsonFormat extends BigQueryJsonFormat[Unit] {
    def write(x: Unit) = JsNumber(1)
    def read(value: JsValue): Unit = {}
  }

  implicit object BooleanJsonFormat extends BigQueryJsonFormat[Boolean] {
    def write(x: Boolean) = JsBoolean(x)
    def read(value: JsValue) = value match {
      case JsTrue | JsString("true") => true
      case JsFalse | JsString("false") => false
      case x => deserializationError("Expected Boolean as JsBoolean or JsString, but got " + x)
    }
  }

  implicit object CharJsonFormat extends BigQueryJsonFormat[Char] {
    def write(x: Char) = JsString(String.valueOf(x))
    def read(value: JsValue) = value match {
      case JsString(x) if x.length == 1 => x.charAt(0)
      case x => deserializationError("Expected Char as single-character JsString, but got " + x)
    }
  }

  implicit object StringJsonFormat extends BigQueryJsonFormat[String] {
    def write(x: String) = {
      require(x ne null)
      JsString(x)
    }
    def read(value: JsValue) = value match {
      case JsString(x) => x
      case x => deserializationError("Expected String as JsString, but got " + x)
    }
  }

  implicit object SymbolJsonFormat extends BigQueryJsonFormat[Symbol] {
    def write(x: Symbol) = JsString(x.name)
    def read(value: JsValue) = value match {
      case JsString(x) => Symbol(x)
      case x => deserializationError("Expected Symbol as JsString, but got " + x)
    }
  }

  implicit object ByteStringJsonFormat extends BigQueryJsonFormat[ByteString] {
    import java.nio.charset.StandardCharsets.US_ASCII
    def write(x: ByteString) = JsString(x.encodeBase64.decodeString(US_ASCII))
    def read(value: JsValue) = value match {
      case BigQueryBytes(x) => x
      case x => deserializationError("Expected ByteString as JsString, but got " + x)
    }
  }
}
