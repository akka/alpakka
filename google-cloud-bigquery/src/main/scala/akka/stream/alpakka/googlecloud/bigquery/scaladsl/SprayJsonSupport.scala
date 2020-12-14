/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.http.scaladsl.unmarshalling.Unmarshaller
import spray.json.{JsValue, RootJsonReader}

object SprayJsonSupport extends SprayJsonSupport

trait SprayJsonSupport extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport {

  implicit def fromJsValueUnmarshaller[T](implicit reader: RootJsonReader[T]): Unmarshaller[JsValue, T] =
    Unmarshaller.strict(_.convertTo[T])

}
