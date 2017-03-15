/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.{Response, ResponseListener, RestClient}

import scala.collection.JavaConverters._

final case class ElasticsearchSinkSettings()

final class ElasticsearchSinkStage(indexName: String,
                                   typeName: String,
                                   client: RestClient,
                                   settings: ElasticsearchSinkSettings)
    extends GraphStage[SinkShape[Map[String, Any]]] {

  val in = Inlet[Map[String, Any]]("ElasticsearchSink.in")

  override def shape: SinkShape[Map[String, Any]] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      override def preStart(): Unit =
        pull(in)

      setHandler(in,
        new InHandler {
        override def onPush(): Unit = {
          val map = grab(in)
          try {
            client.performRequest(
              "POST",
              s"$indexName/$typeName",
              Map[String, String]().asJava,
              new StringEntity(JsonUtils.serialize(map))
            )

            pull(in)

          } catch {
            case ex: Exception => failStage(ex)
          }

        }

        override def onUpstreamFailure(exception: Throwable): Unit =
          failStage(exception)

        override def onUpstreamFinish(): Unit =
          completeStage()
      })
    }

}
