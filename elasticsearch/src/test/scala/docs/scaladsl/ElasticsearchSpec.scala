/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.time.Duration

import akka.actor.ActorSystem
import akka.stream.alpakka.elasticsearch._
//import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.scalatest.{BeforeAndAfterAll, Inspectors}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.testcontainers.elasticsearch.ElasticsearchContainer

class ElasticsearchSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Inspectors
//    with LogCapturing
    with ElasticsearchConnectorBehaviour
    with BeforeAndAfterAll {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-mat

  val containerV6 =
    new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:6.8.0")
      .withStartupTimeout(Duration.ofSeconds(240))
  lazy val clientV6 = RestClient.builder(HttpHost.create(containerV6.getHttpHostAddress)).build()
  val containerV7 =
    new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.6.0")
      .withStartupTimeout(Duration.ofSeconds(240))
  lazy val clientV7 = RestClient.builder(HttpHost.create(containerV7.getHttpHostAddress)).build()

  override def afterAll() = {
    containerV6.stop()
    clientV6.close()
    containerV7.stop()
    clientV7.close()
    TestKit.shutdownActorSystem(system)
  }

  containerV6.start()
  containerV7.start()

  "Connector with ApiVersion 5 running against Elasticsearch v6.8.0" should {
    behave like elasticsearchConnector(ApiVersion.V5, clientV6)
  }

  "Connector with ApiVersion 7 running against Elasticsearch v7.6.0" should {
    behave like elasticsearchConnector(ApiVersion.V7, clientV7)
  }

}
