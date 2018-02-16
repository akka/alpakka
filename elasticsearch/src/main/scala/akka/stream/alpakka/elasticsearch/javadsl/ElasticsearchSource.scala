/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl.Source
import org.elasticsearch.client.RestClient
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, NumericNode}

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

/**
 * Java API to create Elasticsearch sources.
 */
object ElasticsearchSource {

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[OutgoingMessage]]s of [[java.util.Map]].
   * Using default objectMapper
   */
  def create(indexName: String,
             typeName: String,
             query: String,
             settings: ElasticsearchSourceSettings,
             client: RestClient): Source[OutgoingMessage[java.util.Map[String, Object]], NotUsed] =
    create(indexName, typeName, query, settings, client, new ObjectMapper())

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[OutgoingMessage]]s of [[java.util.Map]].
   * Using custom objectMapper
   */
  def create(indexName: String,
             typeName: String,
             query: String,
             settings: ElasticsearchSourceSettings,
             client: RestClient,
             objectMapper: ObjectMapper): Source[OutgoingMessage[java.util.Map[String, Object]], NotUsed] =
    Source.fromGraph(
      new ElasticsearchSourceStage(
        indexName,
        Option(typeName),
        Map("query" -> query),
        client,
        settings.asScala,
        new JacksonReader[java.util.Map[String, Object]](objectMapper, classOf[java.util.Map[String, Object]])
      )
    )

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[OutgoingMessage]]s of [[java.util.Map]].
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
             searchParams: JMap[String, String],
             settings: ElasticsearchSourceSettings,
             client: RestClient,
             objectMapper: ObjectMapper): Source[OutgoingMessage[java.util.Map[String, Object]], NotUsed] =
    Source.fromGraph(
      new ElasticsearchSourceStage(
        indexName,
        Option(typeName),
        searchParams.asScala.toMap,
        client,
        settings.asScala,
        new JacksonReader[java.util.Map[String, Object]](objectMapper, classOf[java.util.Map[String, Object]])
      )
    )

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[OutgoingMessage]]s of type `T`.
   * Using default objectMapper
   */
  def typed[T](indexName: String,
               typeName: String,
               query: String,
               settings: ElasticsearchSourceSettings,
               client: RestClient,
               clazz: Class[T]): Source[OutgoingMessage[T], NotUsed] =
    typed[T](indexName, typeName, query, settings, client, clazz, new ObjectMapper())

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[OutgoingMessage]]s of type `T`.
   * Using custom objectMapper
   */
  def typed[T](indexName: String,
               typeName: String,
               query: String,
               settings: ElasticsearchSourceSettings,
               client: RestClient,
               clazz: Class[T],
               objectMapper: ObjectMapper): Source[OutgoingMessage[T], NotUsed] =
    Source.fromGraph(
      new ElasticsearchSourceStage(
        indexName,
        Option(typeName),
        Map("query" -> query),
        client,
        settings.asScala,
        new JacksonReader[T](objectMapper, clazz)
      )
    )

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[OutgoingMessage]]s of type `T`.
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
               searchParams: JMap[String, String],
               settings: ElasticsearchSourceSettings,
               client: RestClient,
               clazz: Class[T],
               objectMapper: ObjectMapper): Source[OutgoingMessage[T], NotUsed] =
    Source.fromGraph(
      new ElasticsearchSourceStage(
        indexName,
        Option(typeName),
        searchParams.asScala.toMap,
        client,
        settings.asScala,
        new JacksonReader[T](objectMapper, clazz)
      )
    )

  private class JacksonReader[T](mapper: ObjectMapper, clazz: Class[T]) extends MessageReader[T] {

    override def convert(json: String): ScrollResponse[T] = {

      val jsonTree = mapper.readTree(json)

      if (jsonTree.has("error")) {
        ScrollResponse(Some(jsonTree.get("error").asText()), None)
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

          OutgoingMessage[T](id, mapper.treeToValue(source, clazz), version)
        }
        ScrollResponse(None, Some(ScrollResult(scrollId, messages)))
      }
    }
  }

}
