/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch.Operation.{Create, Delete, Index, Update, Upsert}
import akka.stream.alpakka.elasticsearch.{MessageWriter, WriteMessage, WriteResult}
import akka.stream.stage.StageLogging
import spray.json._

import scala.collection.immutable

/**
 * Internal API.
 */
@InternalApi
trait ElasticsearchJsonBase[T, C] {

  self: StageLogging =>

  def indexName: String
  def typeName: String
  def versionType: Option[String]
  def messageWriter: MessageWriter[T]

  private lazy val typeNameTuple = "_type" -> JsString(typeName)
  private lazy val versionTypeTuple: Option[(String, JsString)] = versionType.map { versionType =>
    "version_type" -> JsString(versionType)
  }

  protected def updateJson(messages: immutable.Seq[WriteMessage[T, C]]): String =
    messages
      .map { message =>
        val sharedFields: Seq[(String, JsString)] = Seq(
            "_index" -> JsString(message.indexName.getOrElse(indexName)),
            typeNameTuple
          ) ++ message.customMetadata.map { case (field, value) => field -> JsString(value) }
        val tuple: (String, JsObject) = message.operation match {
          case Index =>
            val fields = Seq(
              message.version.map { version =>
                "_version" -> JsNumber(version)
              },
              versionTypeTuple,
              message.id.map { id =>
                "_id" -> JsString(id)
              }
            ).flatten
            "index" -> JsObject(
              (sharedFields ++ fields): _*
            )
          case Create =>
            val fields = Seq(
              message.id.map { id =>
                "_id" -> JsString(id)
              }
            ).flatten
            "create" -> JsObject(
              (sharedFields ++ fields): _*
            )
          case Update | Upsert =>
            val fields = Seq(
              message.version.map { version =>
                "_version" -> JsNumber(version)
              },
              versionTypeTuple,
              Option("_id" -> JsString(message.id.get))
            ).flatten
            "update" -> JsObject(
              (sharedFields ++ fields): _*
            )
          case Delete =>
            val fields = Seq(
              message.version.map { version =>
                "_version" -> JsNumber(version)
              },
              versionTypeTuple,
              Option("_id" -> JsString(message.id.get))
            ).flatten
            "delete" -> JsObject(
              (sharedFields ++ fields): _*
            )
        }
        JsObject(tuple).compactPrint + messageToJsonString(message)
      }
      .mkString("", "\n", "\n")

  private def messageToJsonString(message: WriteMessage[T, C]): String =
    message.operation match {
      case Index | Create =>
        "\n" + messageWriter.convert(message.source.get)
      case Upsert =>
        "\n" + JsObject(
          "doc" -> messageWriter.convert(message.source.get).parseJson,
          "doc_as_upsert" -> JsTrue
        ).toString
      case Update =>
        "\n" + JsObject(
          "doc" -> messageWriter.convert(message.source.get).parseJson
        ).toString
      case Delete =>
        ""
    }

  protected def writeResults(messages: immutable.Seq[WriteMessage[T, C]],
                             jsonString: String): immutable.Seq[WriteResult[T, C]] = {
    val responseJson = jsonString.parseJson
    if (log.isDebugEnabled) log.debug("response {}", responseJson.prettyPrint)

    // If some commands in bulk request failed, pass failed messages to follows.
    val items = responseJson.asJsObject.fields("items").asInstanceOf[JsArray]
    val messageResults: immutable.Seq[WriteResult[T, C]] = items.elements.zip(messages).map {
      case (item, message) =>
        val command = message.operation.command
        val res = item.asJsObject.fields(command).asJsObject
        val error: Option[String] = res.fields.get("error").map(_.toString())
        new WriteResult(message, error)
    }
    messageResults
  }

}
