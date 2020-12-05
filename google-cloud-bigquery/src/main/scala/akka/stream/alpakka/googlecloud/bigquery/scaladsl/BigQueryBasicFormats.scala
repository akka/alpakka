/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import spray.json.{deserializationError, JsBoolean, JsFalse, JsNumber, JsString, JsTrue, JsValue, JsonFormat}

/**
 * Provides the JsonFormats for the most important Scala types.
 */
trait BigQueryBasicFormats {

  implicit object IntJsonFormat extends JsonFormat[Int] {
    def write(x: Int) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.intValue
      case JsString(x) =>
        try x.toInt
        catch {
          case e: NumberFormatException =>
            deserializationError("Expected Int as JsString, but got " + x)
        }
      case x => deserializationError("Expected Int as JsString, but got " + x)
    }
  }

  implicit object LongJsonFormat extends JsonFormat[Long] {
    def write(x: Long) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.longValue
      case JsString(x) =>
        try x.toLong
        catch {
          case e: NumberFormatException =>
            deserializationError("Expected Long as JsString, but got " + x)
        }
      case x => deserializationError("Expected Long as JsString, but got " + x)
    }
  }

  implicit object FloatJsonFormat extends JsonFormat[Float] {
    def write(x: Float) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.floatValue
      case JsString(x) =>
        try x.toFloat
        catch {
          case e: NumberFormatException =>
            deserializationError("Expected Float as JsString, but got " + x)
        }
      case x => deserializationError("Expected Float as JsString, but got " + x)
    }
  }

  implicit object DoubleJsonFormat extends JsonFormat[Double] {
    def write(x: Double) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.doubleValue
      case JsString(x) =>
        try x.toDouble
        catch {
          case e: NumberFormatException =>
            deserializationError("Expected Double as JsString, but got " + x)
        }
      case x => deserializationError("Expected Double as JsString, but got " + x)
    }
  }

  implicit object ByteJsonFormat extends JsonFormat[Byte] {
    def write(x: Byte) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.byteValue
      case JsString(x) =>
        try x.toByte
        catch {
          case e: NumberFormatException =>
            deserializationError("Expected Byte as JsString, but got " + x)
        }
      case x => deserializationError("Expected Byte as JsString, but got " + x)
    }
  }

  implicit object ShortJsonFormat extends JsonFormat[Short] {
    def write(x: Short) = JsNumber(x)
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.shortValue
      case JsString(x) =>
        try x.toShort
        catch {
          case e: NumberFormatException =>
            deserializationError("Expected Short as JsString, but got " + x)
        }
      case x => deserializationError("Expected Short as JsString, but got " + x)
    }
  }

  implicit object BigDecimalJsonFormat extends JsonFormat[BigDecimal] {
    def write(x: BigDecimal) = {
      require(x ne null)
      JsNumber(x)
    }
    def read(value: JsValue) = value match {
      case JsNumber(x) => x
      case JsString(x) => BigDecimal(x)
      case x => deserializationError("Expected BigDecimal as JsString, but got " + x)
    }
  }

  implicit object BigIntJsonFormat extends JsonFormat[BigInt] {
    def write(x: BigInt) = {
      require(x ne null)
      JsNumber(x)
    }
    def read(value: JsValue) = value match {
      case JsNumber(x) => x.toBigInt
      case JsString(x) => BigInt(x)
      case x => deserializationError("Expected BigInt as JsString, but got " + x)
    }
  }

  implicit object UnitJsonFormat extends JsonFormat[Unit] {
    def write(x: Unit) = JsNumber(1)
    def read(value: JsValue): Unit = {}
  }

  implicit object BooleanJsonFormat extends JsonFormat[Boolean] {
    def write(x: Boolean) = JsBoolean(x)
    def read(value: JsValue) = value match {
      case JsTrue | JsString("true") => true
      case JsFalse | JsString("false") => false
      case x => deserializationError("Expected Boolean as JsString, but got " + x)
    }
  }

  implicit object CharJsonFormat extends JsonFormat[Char] {
    def write(x: Char) = JsString(String.valueOf(x))
    def read(value: JsValue) = value match {
      case JsString(x) if x.length == 1 => x.charAt(0)
      case x => deserializationError("Expected Char as single-character JsString, but got " + x)
    }
  }

  implicit object StringJsonFormat extends JsonFormat[String] {
    def write(x: String) = {
      require(x ne null)
      JsString(x)
    }
    def read(value: JsValue) = value match {
      case JsString(x) => x
      case x => deserializationError("Expected String as JsString, but got " + x)
    }
  }

  implicit object SymbolJsonFormat extends JsonFormat[Symbol] {
    def write(x: Symbol) = JsString(x.name)
    def read(value: JsValue) = value match {
      case JsString(x) => Symbol(x)
      case x => deserializationError("Expected Symbol as JsString, but got " + x)
    }
  }
}
