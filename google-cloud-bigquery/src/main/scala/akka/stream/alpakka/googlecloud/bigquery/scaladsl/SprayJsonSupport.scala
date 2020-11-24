/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.Materializer
import spray.json.{JsValue, RootJsonReader}

import scala.concurrent.{ExecutionContext, Future}

object SprayJsonSupport extends SprayJsonSupport

trait SprayJsonSupport extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport {

  implicit def fromJsValueUnmarshaller[T](implicit reader: RootJsonReader[T]): Unmarshaller[JsValue, T] =
    new Unmarshaller[JsValue, T] {
      override def apply(value: JsValue)(implicit ec: ExecutionContext, materializer: Materializer): Future[T] = {
        Future(value.convertTo[T])
      }
    }

}
