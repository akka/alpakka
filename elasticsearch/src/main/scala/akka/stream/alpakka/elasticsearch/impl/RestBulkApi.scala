/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch.Operation.{Create, Delete, Index, Update, Upsert}
import akka.stream.alpakka.elasticsearch.{WriteMessage, WriteResult}
import spray.json._

import scala.collection.immutable

/**
 * Internal API.
 */
@InternalApi
private[impl] abstract class RestBulkApi[T, C] {

  def toJson(messages: immutable.Seq[WriteMessage[T, C]]): String

  def toWriteResults(messages: immutable.Seq[WriteMessage[T, C]],
                     jsonString: String
  ): immutable.Seq[WriteResult[T, C]] = {
    val responseJson = jsonString.parseJson

    // If some commands in bulk request failed, pass failed messages to follows.
    val items = responseJson.asJsObject.fields("items").asInstanceOf[JsArray]
    val messageResults: immutable.Seq[WriteResult[T, C]] = items.elements.zip(messages).map { case (item, message) =>
      val command = message.operation.command
      val res = item.asJsObject.fields(command).asJsObject
      val error: Option[String] = res.fields.get("error").map(_.toString())
      new WriteResult(message, error)
    }
    messageResults
  }

  def optionalString(fieldName: String, value: Option[String]): Option[(String, JsString)] =
    value.map(v => fieldName -> JsString(v))

  def optionalNumber(fieldName: String, value: Option[Long]): Option[(String, JsNumber)] =
    value.map(v => fieldName -> JsNumber(v))

  def messageToJson(message: WriteMessage[T, C], messageSource: String): String = message.operation match {
    case Index | Create => "\n" + messageSource
    case Upsert => "\n" + JsObject("doc" -> messageSource.parseJson, "doc_as_upsert" -> JsTrue).toString
    case Update => "\n" + JsObject("doc" -> messageSource.parseJson).toString
    case Delete => ""
  }

  def constructSharedFields(message: WriteMessage[T, C]): Seq[(String, JsString)]
}
