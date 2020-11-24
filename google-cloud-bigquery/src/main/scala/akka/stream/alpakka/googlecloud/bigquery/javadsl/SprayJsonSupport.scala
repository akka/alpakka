/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl

import akka.http.javadsl.model.HttpEntity
import akka.http.javadsl.unmarshalling.Unmarshaller
import akka.stream.alpakka.googlecloud.bigquery.BigQueryJsonProtocol
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{SprayJsonSupport => ScalaJsonSupport}
import spray.json.JsValue

object SprayJsonSupport extends {

  def jsValueUnmarshaller: Unmarshaller[HttpEntity, JsValue] = {
    // TODO Can we avoid the cast?
    ScalaJsonSupport.sprayJsValueUnmarshaller.asInstanceOf[Unmarshaller[HttpEntity, JsValue]]
  }

  def responseUnmarshaller: Unmarshaller[JsValue, BigQueryJsonProtocol.Response] = {
    import BigQueryJsonProtocol._
    ScalaJsonSupport.fromJsValueUnmarshaller[Response]
  }

}
