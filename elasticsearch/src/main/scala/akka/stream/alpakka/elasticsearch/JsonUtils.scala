/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag

object JsonUtils {

  private val mapper = new ObjectMapper()
  mapper.enable(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  def serialize(doc: AnyRef): String = mapper.writeValueAsString(doc)

  def deserialize[T](json: String)(implicit c: ClassTag[T]): T =
    if (c.runtimeClass == classOf[Unit]) {
      ().asInstanceOf[T]
    } else {
      mapper.readValue(json, c.runtimeClass).asInstanceOf[T]
    }

}
