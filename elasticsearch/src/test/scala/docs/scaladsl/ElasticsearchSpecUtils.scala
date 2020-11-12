/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{ContentTypes, HttpMethods, HttpRequest, Uri}
import akka.stream.Materializer
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSource
import akka.stream.alpakka.elasticsearch.{
  ApiVersion,
  ElasticsearchConnectionSettings,
  ElasticsearchParams,
  ElasticsearchSourceSettings
}
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable
import scala.concurrent.Future

trait ElasticsearchSpecUtils { this: AnyWordSpec with ScalaFutures =>
  implicit def materializer: Materializer

  def http: HttpExt

  //#define-class
  import spray.json._
  import DefaultJsonProtocol._

  case class Book(title: String)

  implicit val format: JsonFormat[Book] = jsonFormat1(Book)
  //#define-class

  def register(connectionSettings: ElasticsearchConnectionSettings, indexName: String, title: String): Unit = {
    val request = HttpRequest(HttpMethods.POST)
      .withUri(Uri(connectionSettings.baseUrl).withPath(Path(s"/$indexName/_doc")))
      .withEntity(ContentTypes.`application/json`, s"""{"title": "$title"}""")
    http.singleRequest(request).futureValue
  }

  def flushAndRefresh(connectionSettings: ElasticsearchConnectionSettings, indexName: String): Unit = {
    val flushRequest = HttpRequest(HttpMethods.POST)
      .withUri(Uri(connectionSettings.baseUrl).withPath(Path(s"/$indexName/_flush")))
    http.singleRequest(flushRequest).futureValue

    val refreshRequest = HttpRequest(HttpMethods.POST)
      .withUri(Uri(connectionSettings.baseUrl).withPath(Path(s"/$indexName/_refresh")))
    http.singleRequest(refreshRequest).futureValue
  }

  def readTitlesFrom(apiVersion: ApiVersion,
                     sourceSettings: ElasticsearchSourceSettings,
                     indexName: String): Future[immutable.Seq[String]] =
    ElasticsearchSource
      .typed[Book](
        constructElasticsearchParams(indexName, "_doc", apiVersion),
        query = """{"match_all": {}}""",
        settings = sourceSettings
      )
      .map { message =>
        message.source.title
      }
      .runWith(Sink.seq)

  def insertTestData(connectionSettings: ElasticsearchConnectionSettings): Unit = {
    register(connectionSettings, "source", "Akka in Action")
    register(connectionSettings, "source", "Programming in Scala")
    register(connectionSettings, "source", "Learning Scala")
    register(connectionSettings, "source", "Scala for Spark in Production")
    register(connectionSettings, "source", "Scala Puzzlers")
    register(connectionSettings, "source", "Effective Akka")
    register(connectionSettings, "source", "Akka Concurrency")
    flushAndRefresh(connectionSettings, "source")
  }

  def constructElasticsearchParams(indexName: String, typeName: String, apiVersion: ApiVersion): ElasticsearchParams = {
    if (apiVersion == ApiVersion.V5) {
      ElasticsearchParams.V5(indexName, typeName)
    } else {
      ElasticsearchParams.V7(indexName)
    }
  }
}
