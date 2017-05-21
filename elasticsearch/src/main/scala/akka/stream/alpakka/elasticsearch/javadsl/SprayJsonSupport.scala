/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch.javadsl

import spray.json._

import scala.collection.mutable

/**
 * Helpers to use spray-json in Java.
 */
object SprayJsonSupport {

  sealed trait JsonValue

  case class JsonObject(json: JsObject) extends JsonValue {
    private val fields = new mutable.HashMap[String, JsonField]()

    if (json != null) {
      json.fields.foreach {
        case (name, value) =>
          fields += name -> JsonField(name, toJsonValue(value))
      }
    }

    def this() = this(null)

    def getField(name: String): JsonField =
      fields.get(name).orNull

    def getFields(): Array[JsonField] = fields.values.toArray

    def addField(field: JsonField): JsonObject = {
      fields += field.name -> field
      this
    }

    def toJson(): JsObject =
      JsObject(fields.toSeq.map {
        case (_, field) =>
          field.name -> toJsValue(field.value)
      }: _*)
  }

  case object JsonNull extends JsonValue

  case class JsonString(value: String) extends JsonValue

  case class JsonNumber(value: java.math.BigDecimal) extends JsonValue {
    def this(value: java.lang.Integer) = this(new java.math.BigDecimal(value))
    def this(value: java.lang.Long) = this(new java.math.BigDecimal(value))
    def this(value: java.lang.Double) = this(new java.math.BigDecimal(value))
    def intValue(): java.lang.Integer = value.intValue()
    def longValue(): java.lang.Long = value.longValue()
    def doubleValue(): java.lang.Double = value.doubleValue()
  }

  case class JsonBoolean(value: Boolean) extends JsonValue

  case class JsonField(name: String, value: JsonValue) {
    def getStringValue(): String = value.asInstanceOf[JsonString].value
    def getIntegerValue(): java.lang.Integer = value.asInstanceOf[JsonNumber].intValue()
    def getLongValue(): java.lang.Long = value.asInstanceOf[JsonNumber].longValue()
    def getDoubleValue(): java.lang.Double = value.asInstanceOf[JsonNumber].doubleValue()
    def getBooleanValue(): java.lang.Boolean = value.asInstanceOf[JsonBoolean].value
  }

  case class JsonArray(value: Seq[JsonValue]) extends JsonValue

  private def toJsonValue(value: JsValue): JsonValue =
    value match {
      case x: JsNull.type => JsonNull
      case x: JsObject => JsonObject(x)
      case x: JsString => JsonString(x.value)
      case x: JsNumber => JsonNumber(x.value.bigDecimal)
      case x: JsBoolean => JsonBoolean(x.value)
      case x: JsArray =>
        JsonArray(x.elements.map { e =>
          toJsonValue(e)
        })
    }

  private def toJsValue(value: JsonValue): JsValue =
    value match {
      case x: JsonNull.type => JsNull
      case x: JsonObject => x.toJson()
      case x: JsonString => JsString(x.value)
      case x: JsonNumber => JsNumber(x.value)
      case x: JsonBoolean => JsBoolean(x.value)
      case x: JsonArray =>
        JsArray(x.value.map { e =>
          toJsValue(e)
        }: _*)
    }

}
