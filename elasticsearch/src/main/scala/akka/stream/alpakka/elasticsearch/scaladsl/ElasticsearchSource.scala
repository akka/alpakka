/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.alpakka.common.SourceSettings
import akka.stream.alpakka.elasticsearch.{impl, _}
import akka.stream.scaladsl.Source
import spray.json._

import scala.concurrent.ExecutionContextExecutor

/**
 * Scala API to create Elasticsearch sources.
 */
object ElasticsearchSource {

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s
   * of Spray's [[spray.json.JsObject]].
   * Alias of [[create]].
   */
  def apply(
      elasticsearchParams: ElasticsearchParams,
      query: String,
      settings: SourceSettings[_, _]
  ): Source[ReadResult[JsObject], NotUsed] = create(elasticsearchParams, query, settings)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s
   * of Spray's [[spray.json.JsObject]].
   * Alias of [[create]].
   *
   * Example of searchParams-usage:
   *  Map( "query" -> """{"match_all": {}}""" )
   *  Map( "query" -> """{"match_all": {}}""", "_source" -> """ ["fieldToInclude", "anotherFieldToInclude"] """ )
   */
  def apply(elasticsearchParams: ElasticsearchParams,
            searchParams: Map[String, String],
            settings: SourceSettings[_, _]): Source[ReadResult[JsObject], NotUsed] =
    create(elasticsearchParams, searchParams, settings)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s
   * of Spray's [[spray.json.JsObject]].
   */
  def create(elasticsearchParams: ElasticsearchParams,
             query: String,
             settings: SourceSettings[_, _]): Source[ReadResult[JsObject], NotUsed] =
    create(elasticsearchParams, Map("query" -> query), settings)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s
   * of Spray's [[spray.json.JsObject]].
   *
   * Example of searchParams-usage:
   *  Map( "query" -> """{"match_all": {}}""" )
   *  Map( "query" -> """{"match_all": {}}""", "_source" -> """ ["fieldToInclude", "anotherFieldToInclude"] """ )
   */
  def create(elasticsearchParams: ElasticsearchParams,
             searchParams: Map[String, String],
             settings: SourceSettings[_, _]): Source[ReadResult[JsObject], NotUsed] =
    Source
      .fromMaterializer { (mat, _) =>
        implicit val system: ActorSystem = mat.system
        implicit val http: HttpExt = Http()
        implicit val ec: ExecutionContextExecutor = mat.executionContext

        val sourceStage = new impl.ElasticsearchSourceStage(
          elasticsearchParams,
          searchParams,
          settings,
          new SprayJsonReader[JsObject]()(DefaultJsonProtocol.RootJsObjectFormat)
        )

        Source.fromGraph(sourceStage)
      }
      .mapMaterializedValue(_ => NotUsed)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`
   * converted by Spray's [[spray.json.JsonReader]]
   */
  def typed[T](elasticsearchParams: ElasticsearchParams, query: String, settings: SourceSettings[_, _])(
      implicit sprayJsonReader: JsonReader[T]
  ): Source[ReadResult[T], NotUsed] =
    typed(elasticsearchParams, Map("query" -> query), settings)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`
   * converted by Spray's [[spray.json.JsonReader]]
   *
   * Example of searchParams-usage:
   *  Map( "query" -> """{"match_all": {}}""" )
   *  Map( "query" -> """{"match_all": {}}""", "_source" -> """ ["fieldToInclude", "anotherFieldToInclude"] """ )
   */
  def typed[T](elasticsearchParams: ElasticsearchParams,
               searchParams: Map[String, String],
               settings: SourceSettings[_, _])(
      implicit sprayJsonReader: JsonReader[T]
  ): Source[ReadResult[T], NotUsed] =
    Source
      .fromMaterializer { (mat, _) =>
        implicit val system: ActorSystem = mat.system
        implicit val http: HttpExt = Http()
        implicit val ec: ExecutionContextExecutor = mat.executionContext

        Source.fromGraph(
          new impl.ElasticsearchSourceStage(elasticsearchParams,
                                            searchParams,
                                            settings,
                                            new SprayJsonReader[T]()(sprayJsonReader))
        )
      }
      .mapMaterializedValue(_ => NotUsed)

  private final class SprayJsonReader[T](implicit reader: JsonReader[T]) extends impl.MessageReader[T] {

    override def convert(json: String): impl.ScrollResponse[T] = {
      val jsObj = json.parseJson.asJsObject
      jsObj.fields.get("error") match {
        case Some(error) => {
          impl.ScrollResponse(Some(error.toString), None)
        }
        case None => {
          val scrollId = jsObj.fields.get("_scroll_id").map(v => v.asInstanceOf[JsString].value)
          val hits = jsObj.fields("hits").asJsObject.fields("hits").asInstanceOf[JsArray]
          val messages = hits.elements.map { element =>
            val doc = element.asJsObject
            val id = doc.fields("_id").asInstanceOf[JsString].value
            val source = doc.fields("_source").asJsObject
            // Maybe we got the _version-property
            val version: Option[Long] = doc.fields.get("_version").map(_.asInstanceOf[JsNumber].value.toLong)
            new ReadResult(id, source.convertTo[T], version)
          }
          impl.ScrollResponse(None, Some(impl.ScrollResult(scrollId, messages)))
        }
      }
    }

  }

}
