/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl.Source
import org.elasticsearch.client.RestClient
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._

object ElasticsearchSource {

  /**
   * Java API: creates a [[ElasticsearchSourceStage]] that consumes as java.util.Map
   */
  def create(indexName: String,
             typeName: String,
             query: String,
             settings: ElasticsearchSourceSettings,
             client: RestClient): Source[OutgoingMessage[java.util.Map[String, Object]], NotUsed] =
    Source.fromGraph(
      new ElasticsearchSourceStage(
        indexName,
        typeName,
        query,
        client,
        settings.asScala,
        new JacksonReader[java.util.Map[String, Object]](classOf[java.util.Map[String, Object]])
      )
    )

  /**
   * Java API: creates a [[ElasticsearchSourceStage]] that consumes as specific type
   */
  def typed[T](indexName: String,
               typeName: String,
               query: String,
               settings: ElasticsearchSourceSettings,
               client: RestClient,
               clazz: Class[T]): Source[OutgoingMessage[T], NotUsed] =
    Source.fromGraph(
      new ElasticsearchSourceStage(
        indexName,
        typeName,
        query,
        client,
        settings.asScala,
        new JacksonReader[T](clazz)
      )
    )

  private class JacksonReader[T](clazz: Class[T]) extends MessageReader[T] {

    private val mapper = new ObjectMapper()

    override def convert(json: String): ScrollResponse[T] = {
      val map = mapper.readValue(json, classOf[java.util.Map[String, Object]])
      val error = map.get("error")
      if (error != null) {
        ScrollResponse(Some(error.toString), None)
      } else {
        val scrollId = map.get("_scroll_id").asInstanceOf[String]
        val hits = map
          .get("hits")
          .asInstanceOf[java.util.Map[String, Object]]
          .get("hits")
          .asInstanceOf[java.util.List[java.util.Map[String, Object]]]
        val messages = hits.asScala.map { element =>
          val id = element.get("_id").asInstanceOf[String]
          val source = element.get("_source").asInstanceOf[java.util.Map[String, Object]]
          if (clazz.isAssignableFrom(classOf[java.util.Map[String, Object]])) {
            OutgoingMessage[T](id, source.asInstanceOf[T])
          } else {
            val obj = mapper.readValue(mapper.writeValueAsString(source), clazz)
            OutgoingMessage[T](id, obj)
          }
        }
        ScrollResponse(None, Some(ScrollResult(scrollId, messages)))
      }
    }
  }

}
