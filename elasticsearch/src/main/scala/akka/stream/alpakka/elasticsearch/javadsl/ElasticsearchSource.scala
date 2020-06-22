/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import java.util.function.BiFunction

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl.Source
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, NumericNode}
import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.alpakka.elasticsearch.impl

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/**
 * Java API to create Elasticsearch sources.
 */
object ElasticsearchSource {

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of [[java.util.Map]].
   * Using default objectMapper
   */
  def create(indexName: String,
             typeName: String,
             query: String,
             settings: ElasticsearchSourceSettings): Source[ReadResult[java.util.Map[String, Object]], NotUsed] =
    create(indexName, typeName, query, settings, new ObjectMapper())

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of [[java.util.Map]].
   * Using custom objectMapper
   */
  def create(indexName: String,
             typeName: String,
             query: String,
             settings: ElasticsearchSourceSettings,
             objectMapper: ObjectMapper): Source[ReadResult[java.util.Map[String, Object]], NotUsed] = {
    val indexType = ElasticsearchIndexType(indexName, Option(typeName), settings.apiVersion)

    Source
      .setup {
        japiBiFunction((mat: ActorMaterializer, _: Attributes) => {
          implicit val system: ActorSystem = mat.system
          implicit val http: HttpExt = Http()
          implicit val ec: ExecutionContext = mat.executionContext

          Source
            .fromGraph(
              new impl.ElasticsearchSourceStage(
                indexType,
                Map("query" -> query),
                settings,
                new JacksonReader[java.util.Map[String, Object]](objectMapper, classOf[java.util.Map[String, Object]])
              )
            )
        })
      }
      .mapMaterializedValue(japiFunction(_ => NotUsed))
  }

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of [[java.util.Map]].
   * Using custom objectMapper.
   *
   * Example of searchParams-usage:
   *
   * Map<String, String> searchParams = new HashMap<>();
   * searchParams.put("query", "{\"match_all\": {}}");
   * searchParams.put("_source", "[\"fieldToInclude\", \"anotherFieldToInclude\"]");
   */
  def create(indexName: String,
             typeName: String,
             searchParams: java.util.Map[String, String],
             settings: ElasticsearchSourceSettings,
             objectMapper: ObjectMapper): Source[ReadResult[java.util.Map[String, Object]], NotUsed] = {
    val indexType = ElasticsearchIndexType(indexName, Option(typeName), settings.apiVersion)

    Source
      .setup {
        japiBiFunction((mat: ActorMaterializer, _: Attributes) => {
          implicit val system: ActorSystem = mat.system
          implicit val http: HttpExt = Http()
          implicit val ec: ExecutionContext = mat.executionContext

          Source.fromGraph(
            new impl.ElasticsearchSourceStage(
              indexType,
              searchParams.asScala.toMap,
              settings,
              new JacksonReader[java.util.Map[String, Object]](objectMapper, classOf[java.util.Map[String, Object]])
            )
          )
        })
      }
      .mapMaterializedValue(japiFunction(_ => NotUsed))
  }

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using default objectMapper
   */
  def typed[T](indexName: String,
               typeName: String,
               query: String,
               settings: ElasticsearchSourceSettings,
               clazz: Class[T]): Source[ReadResult[T], NotUsed] =
    typed[T](indexName, typeName, query, settings, clazz, new ObjectMapper())

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using custom objectMapper
   */
  def typed[T](indexName: String,
               typeName: String,
               query: String,
               settings: ElasticsearchSourceSettings,
               clazz: Class[T],
               objectMapper: ObjectMapper): Source[ReadResult[T], NotUsed] = {
    val indexType = ElasticsearchIndexType(indexName, Option(typeName), settings.apiVersion)

    Source
      .setup {
        japiBiFunction((mat: ActorMaterializer, _: Attributes) => {
          implicit val system: ActorSystem = mat.system
          implicit val http: HttpExt = Http()
          implicit val ec: ExecutionContext = mat.executionContext

          Source.fromGraph(
            new impl.ElasticsearchSourceStage(
              indexType,
              Map("query" -> query),
              settings,
              new JacksonReader[T](objectMapper, clazz)
            )
          )
        })
      }
      .mapMaterializedValue(japiFunction(_ => NotUsed))
  }

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using custom objectMapper
   *
   * Example of searchParams-usage:
   *
   * Map<String, String> searchParams = new HashMap<>();
   * searchParams.put("query", "{\"match_all\": {}}");
   * searchParams.put("_source", "[\"fieldToInclude\", \"anotherFieldToInclude\"]");
   */
  def typed[T](indexName: String,
               typeName: String,
               searchParams: java.util.Map[String, String],
               settings: ElasticsearchSourceSettings,
               clazz: Class[T],
               objectMapper: ObjectMapper): Source[ReadResult[T], NotUsed] = {
    val indexType = ElasticsearchIndexType(indexName, Option(typeName), settings.apiVersion)

    Source
      .setup {
        japiBiFunction((mat: ActorMaterializer, _: Attributes) => {
          implicit val system: ActorSystem = mat.system
          implicit val http: HttpExt = Http()
          implicit val ec: ExecutionContext = mat.executionContext

          Source.fromGraph(
            new impl.ElasticsearchSourceStage(
              indexType,
              searchParams.asScala.toMap,
              settings,
              new JacksonReader[T](objectMapper, clazz)
            )
          )
        })
      }
      .mapMaterializedValue(japiFunction(_ => NotUsed))
  }

  private final class JacksonReader[T](mapper: ObjectMapper, clazz: Class[T]) extends impl.MessageReader[T] {

    override def convert(json: String): impl.ScrollResponse[T] = {

      val jsonTree = mapper.readTree(json)

      if (jsonTree.has("error")) {
        impl.ScrollResponse(Some(jsonTree.get("error").asText()), None)
      } else {
        val scrollId = jsonTree.get("_scroll_id").asText()
        val hits = jsonTree.get("hits").get("hits").asInstanceOf[ArrayNode]
        val messages = hits.elements().asScala.toList.map { element =>
          val id = element.get("_id").asText()
          val source = element.get("_source")
          val version: Option[Long] = element.get("_version") match {
            case n: NumericNode => Some(n.asLong())
            case _ => None
          }

          new ReadResult[T](id, mapper.treeToValue(source, clazz), version)
        }
        impl.ScrollResponse(None, Some(impl.ScrollResult(scrollId, messages)))
      }
    }
  }

  /**
   * Helper for creating BiFunction instances from Scala
   * functions as Scala 2.11 does not know about SAMs.
   */
  private def japiBiFunction[T, U, R](op: (T, U) => R): BiFunction[T, U, R] = {
    new BiFunction[T, U, R] {
      override def apply(t: T, u: U): R = op.apply(t, u)
    }
  }

  /**
   * Helper for creating akka.japi.function.Function instances from Scala
   * functions as Scala 2.11 does not know about SAMs.
   */
  private def japiFunction[A, B](f: A => B): akka.japi.function.Function[A, B] =
    new akka.japi.function.Function[A, B]() {
      override def apply(a: A): B = f(a)
    }
}
