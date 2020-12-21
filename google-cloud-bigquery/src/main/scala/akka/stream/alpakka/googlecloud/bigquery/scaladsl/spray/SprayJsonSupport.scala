/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import akka.http.scaladsl.marshalling.{Marshaller, ToByteStringMarshaller}
import akka.http.scaladsl.model.MediaTypes
import akka.util.ByteString
import spray.json.{enrichAny, CompactPrinter, JsonPrinter}

object SprayJsonSupport extends SprayJsonSupport

trait SprayJsonSupport extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport {

  implicit def sprayJsonToByteStringMarshaller[T](implicit writer: BigQueryJsonWriter[T],
                                                  printer: JsonPrinter = CompactPrinter): ToByteStringMarshaller[T] =
    Marshaller.withFixedContentType(MediaTypes.`application/json`)(t => ByteString(t.toJson.toString(printer)))

}
