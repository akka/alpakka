/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch.Operation._
import akka.stream.alpakka.elasticsearch.{MessageWriter, WriteMessage}
import spray.json._

import scala.collection.immutable

/**
 * Internal API.
 *
 * REST API implementation for some Elasticsearch 7 version.
 * https://www.elastic.co/guide/en/elasticsearch/reference/7.6/docs-bulk.html
 */
@InternalApi
private[impl] final class RestBulkApiV7[T, C](indexName: String,
                                              versionType: Option[String],
                                              messageWriter: MessageWriter[T])
    extends RestBulkApi[T, C] {

  def toJson(messages: immutable.Seq[WriteMessage[T, C]]): String =
    messages
      .map { message =>
        val sharedFields: Seq[(String, JsString)] = Seq(
            "_index" -> JsString(message.indexName.getOrElse(indexName))
          ) ++ message.customMetadata.map { case (field, value) => field -> JsString(value) }
        val tuple: (String, JsObject) = message.operation match {
          case Index =>
            val fields = Seq(
              optionalNumber("version", message.version),
              optionalString("version_type", versionType),
              optionalString("_id", message.id)
            ).flatten
            "index" -> JsObject(sharedFields ++ fields: _*)
          case Create => "create" -> JsObject(sharedFields ++ optionalString("_id", message.id): _*)
          case Update | Upsert => "update" -> JsObject(sharedFields :+ ("_id" -> JsString(message.id.get)): _*)
          case Delete =>
            val fields =
              ("_id" -> JsString(message.id.get)) +: Seq(
                optionalNumber("version", message.version),
                optionalString("version_type", versionType)
              ).flatten
            "delete" -> JsObject(sharedFields ++ fields: _*)
        }
        JsObject(tuple).compactPrint + messageToJson(message, message.source.fold("")(messageWriter.convert))
      }
      .mkString("", "\n", "\n")

}
