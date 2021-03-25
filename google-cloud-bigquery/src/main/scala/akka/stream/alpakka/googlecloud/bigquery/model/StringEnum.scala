/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.annotation.InternalApi
import spray.json.{deserializationError, JsString, JsValue, JsonFormat}

import scala.reflect.ClassTag

@InternalApi
private[model] abstract class StringEnum {
  def value: String
  final def getValue: String = value
}

@InternalApi
private[model] object StringEnum {
  def jsonFormat[T <: StringEnum](f: String => T)(implicit ct: ClassTag[T]): JsonFormat[T] = new JsonFormat[T] {
    override def write(obj: T): JsValue = JsString(obj.value)
    override def read(json: JsValue): T = json match {
      case JsString(x) => f(x)
      case x => deserializationError(s"Expected ${ct.runtimeClass.getSimpleName} as JsString, but got " + x)
    }
  }
}
