/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Inspectors}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ElasticsearchSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Inspectors
    with LogCapturing
    with ElasticsearchConnectorBehaviour
    with ElasticsearchSpecUtils
    with BeforeAndAfterAll {

  //#init-mat
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-mat
  implicit val http: HttpExt = Http()

  val clientV5: ElasticsearchConnectionSettings =
    ElasticsearchConnectionSettings("http://localhost:9201")
  val clientV7: ElasticsearchConnectionSettings =
    ElasticsearchConnectionSettings("http://localhost:9202")

  override def afterAll() = {
    val deleteRequestV5 = HttpRequest(HttpMethods.DELETE)
      .withUri(Uri(clientV5.baseUrl).withPath(Path("/_all")))
    http.singleRequest(deleteRequestV5).futureValue

    val deleteRequestV7 = HttpRequest(HttpMethods.DELETE)
      .withUri(Uri(clientV7.baseUrl).withPath(Path("/_all")))
    http.singleRequest(deleteRequestV7).futureValue

    TestKit.shutdownActorSystem(system)
  }

  "Connector with ApiVersion 5 running against Elasticsearch v6.8.0" should {
    behave like elasticsearchConnector(ApiVersion.V5, clientV5)
  }

  "Connector with ApiVersion 7 running against Elasticsearch v7.6.0" should {
    behave like elasticsearchConnector(ApiVersion.V7, clientV7)
  }

}
