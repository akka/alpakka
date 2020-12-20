/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import akka.http.scaladsl.marshalling.{Marshaller, ToByteStringMarshaller}
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.util.ByteString
import spray.json.{enrichAny, CompactPrinter, JsValue, JsonPrinter, RootJsonReader}

object SprayJsonSupport extends SprayJsonSupport

trait SprayJsonSupport extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport {

  implicit def fromJsValueUnmarshaller[T](implicit reader: RootJsonReader[T]): Unmarshaller[JsValue, T] =
    Unmarshaller.strict(_.convertTo[T])

  implicit def sprayJsonToByteStringMarshaller[T](implicit writer: BigQueryJsonWriter[T],
                                                  printer: JsonPrinter = CompactPrinter): ToByteStringMarshaller[T] =
    Marshaller.withFixedContentType(MediaTypes.`application/json`)(t => ByteString(t.toJson.toString(printer)))

}
