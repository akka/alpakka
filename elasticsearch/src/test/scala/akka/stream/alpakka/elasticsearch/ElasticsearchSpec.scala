/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.elasticsearch.scaladsl.{ElasticsearchSink, ElasticsearchSource}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import org.apache.http.HttpHost
import org.apache.http.entity.StringEntity
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.elasticsearch.client.RestClient
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import spray.json.JsString

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._

class ElasticsearchSpec extends WordSpec with Matchers with BeforeAndAfter {

  private val runner = new ElasticsearchClusterRunner()

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()
  implicit val client = RestClient.builder(new HttpHost("localhost", 9201)).build()

  before {
    runner.build(ElasticsearchClusterRunner.newConfigs().numOfNode(1))
    runner.ensureYellow()

    register("source", "Akka in Action")
    register("source", "Programming in Scala")
    register("source", "Learning Scala")
    register("source", "Scala for Spark in Production")
    register("source", "Scala Puzzlers")
    register("source", "Effective Akka")
    register("source", "Akka Concurrency")
    flush("source")
  }

  after {
    runner.close()
    client.close()
    TestKit.shutdownActorSystem(system)
  }

  private def flush(indexName: String): Unit =
    client.performRequest("POST", s"$indexName/_flush")

  private def register(indexName: String, title: String): Unit =
    client.performRequest("POST", s"$indexName/book", Map[String, String]().asJava,
      new StringEntity(s"""{"title": "$title"}"""))

  "Elasticsearch connector" should {
    "consume and publish documents through Elasticsearch" in {
      val f1 = ElasticsearchSource(
        "source",
        "book",
        """{"match_all": {}}""",
        ElasticsearchSourceSettings(5)
      ).map { message =>
        IncomingMessage(message.id, message.source)
      }.runWith(ElasticsearchSink(
          "sink",
          "book",
          ElasticsearchSinkSettings()
        ))

      Await.result(f1, Duration.Inf)

      flush("sink")

      val f2 = ElasticsearchSource(
        "sink",
        "book",
        """{"match_all": {}}""",
        ElasticsearchSourceSettings(5)
      ).map { message =>
        message.source.fields("title").asInstanceOf[JsString].value
      }.runWith(Sink.seq)

      val result = Await.result(f2, Duration.Inf)

      result.sorted shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

}
