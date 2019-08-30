/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.NotUsed
import akka.annotation.InternalApi

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] sealed abstract class Operation(val command: String) {
  override def toString: String = command
}

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] object Operation {
  object Index extends Operation("index")
  object Create extends Operation("create")
  object Update extends Operation("update")
  object Upsert extends Operation("update")
  object Delete extends Operation("delete")
  object InlineScript extends Operation("inlineScript")
  object PreparedScript extends Operation("preparedScript")

}

final class WriteMessage[T, PT, U] private (val operation: Operation,
                                         val id: Option[String],
                                         val source: Option[T],
                                         val passThrough: PT = NotUsed,
                                         val version: Option[Long] = None,
                                         val indexName: Option[String] = None,
                                         val customMetadata: Map[String, java.lang.String] = Map.empty,
                                         val lang: Option[String] = None,
                                         val scriptRef: Option[String] = None,
                                         val script: Option[String] =  None,
                                         val upsertDoc: U = NotUsed) {

  def withSource(value: T): WriteMessage[T, PT, U] = copy(source = Option(value))

  def withPassThrough[PT2](value: PT2): WriteMessage[T, PT2, U] =
    new WriteMessage[T, PT2, U](operation = operation,
                             id = id,
                             source = source,
                             value,
                             version = version,
                             indexName = indexName,
                             customMetadata = customMetadata,
                             lang = lang,
                             scriptRef = scriptRef,
                             script = script,
                             upsertDoc = upsertDoc)

  def withVersion(value: Long): WriteMessage[T, PT, U] = copy(version = Option(value))
  def withIndexName(value: String): WriteMessage[T, PT, U] = copy(indexName = Option(value))

  /**
   * Scala API: define custom metadata for this message. Fields should
   * have the full metadata field name as key (including the "_" prefix if there is one)
   */
  def withCustomMetadata(value: Map[String, java.lang.String]): WriteMessage[T, PT, U] = copy(customMetadata = value)

  /**
   * Java API: define custom metadata for this message. Fields should
   * have the full metadata field name as key (including the "_" prefix if there is one)
   */
  def withCustomMetadata(metadata: java.util.Map[String, String]): WriteMessage[T, PT, U] =
    this.copy(customMetadata = metadata.asScala.toMap)

  private def copy(operation: Operation = operation,
                   id: Option[String] = id,
                   source: Option[T] = source,
                   passThrough: PT = passThrough,
                   version: Option[Long] = version,
                   indexName: Option[String] = indexName,
                   customMetadata: Map[String, String] = customMetadata,
                   lang: Option[String] = lang,
                   scriptRef: Option[String] = scriptRef,
                   script: Option[String] = script,
                   upsertDoc: U = upsertDoc): WriteMessage[T, PT, U] =
    new WriteMessage[T, PT, U](operation = operation,
                            id = id,
                            source = source,
                            passThrough = passThrough,
                            version = version,
                            indexName = indexName,
                            customMetadata = customMetadata, lang = lang, upsertDoc = upsertDoc)

  override def toString =
    s"""WriteMessage(operation=$operation,id=$id,source=$source,passThrough=$passThrough,version=$version,indexName=$indexName,customMetadata=$customMetadata)"""

  override def equals(other: Any): Boolean = other match {
    case that: WriteMessage[_, _,_] =>
      java.util.Objects.equals(this.operation, that.operation) &&
      java.util.Objects.equals(this.id, that.id) &&
      java.util.Objects.equals(this.source, that.source) &&
      java.util.Objects.equals(this.passThrough, that.passThrough) &&
      java.util.Objects.equals(this.version, that.version) &&
      java.util.Objects.equals(this.indexName, that.indexName) &&
      java.util.Objects.equals(this.customMetadata, that.customMetadata)
    case _ => false
  }

  override def hashCode(): Int =
    passThrough match {
      case pt: AnyRef =>
        java.util.Objects.hash(operation, id, source, pt, version, indexName, customMetadata)
      case _ =>
        java.util.Objects.hash(operation, id, source, version, indexName, customMetadata)
    }
}

object WriteMessage {
  import Operation._

  def createIndexMessage[T](source: T): WriteMessage[T, NotUsed, NotUsed] =
    new WriteMessage(Index, id = None, source = Option(source))

  def createIndexMessage[T](id: String, source: T): WriteMessage[T, NotUsed, NotUsed] =
    new WriteMessage(Index, id = Option(id), source = Option(source))

  def createCreateMessage[T](id: String, source: T): WriteMessage[T, NotUsed,NotUsed] =
    new WriteMessage(Create, id = Option(id), source = Option(source))

  def createUpdateMessage[T](id: String, source: T): WriteMessage[T, NotUsed,NotUsed] =
    new WriteMessage(Update, id = Option(id), source = Option(source))

  def createUpsertMessage[T](id: String, source: T): WriteMessage[T, NotUsed,NotUsed] =
    new WriteMessage(Upsert, id = Option(id), source = Option(source))

  def createDeleteMessage[T](id: String): WriteMessage[T, NotUsed,NotUsed] =
    new WriteMessage(Delete, id = Option(id), None)

  def createInlineScriptMessage[T](id:String, script: String, lang: String, source:T): WriteMessage[T, NotUsed,NotUsed] =
    new WriteMessage(InlineScript, id = Option(id), source = Option(source), script = Option(script), lang = Option(lang))

  def createPreparedScriptMessage[T](id:String, source:T, scriptRef: String): WriteMessage[T, NotUsed,NotUsed] =
    new WriteMessage(PreparedScript, id = Option(id), source = Option(source), scriptRef = Option(scriptRef))
}

/**
 * Stream element type emitted by Elasticsearch flows.
 *
 * The constructor is INTERNAL API, but you may construct instances for testing by using
 * [[akka.stream.alpakka.elasticsearch.testkit.MessageFactory]].
 */
final class WriteResult[T2, C2] @InternalApi private[elasticsearch] (val message: WriteMessage[T2, C2],
                                                                     /** JSON structure of the Elasticsearch error. */
                                                                     val error: Option[String]) {
  val success: Boolean = error.isEmpty

  /** Java API: JSON structure of the Elasticsearch error. */
  def getError: java.util.Optional[String] = error.asJava

  /** `reason` field value of the Elasticsearch error. */
  def errorReason: Option[String] = {
    import spray.json._
    error.flatMap(_.parseJson.asJsObject.fields.get("reason").map(_.asInstanceOf[JsString].value))
  }

  /** Java API: `reason` field value from the Elasticsearch error */
  def getErrorReason: java.util.Optional[String] = errorReason.asJava

  override def toString =
    s"""WriteResult(message=$message,error=$error)"""

  override def equals(other: Any): Boolean = other match {
    case that: WriteResult[T2, C2] =>
      java.util.Objects.equals(this.message, that.message) &&
      java.util.Objects.equals(this.error, that.error)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(message, error)
}

trait MessageWriter[T] {
  def convert(message: T): String
}

sealed class StringMessageWriter private () extends MessageWriter[String] {
  override def convert(message: String): String = message
}

object StringMessageWriter extends StringMessageWriter {

  /** Java API: get the singleton instance of `StringMessageWriter` */
  val getInstance: StringMessageWriter = StringMessageWriter
}
