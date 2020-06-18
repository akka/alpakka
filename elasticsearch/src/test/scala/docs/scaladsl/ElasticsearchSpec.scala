/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.elasticsearch.client.Request
//#init-client
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
//#init-client
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
    with BeforeAndAfterAll {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  //#init-mat

  //#init-client

  val client = RestClient.builder(new HttpHost("localhost", 9201)).build()
  //#init-client
  val clientV7 = RestClient.builder(new HttpHost("localhost", 9202)).build()

  override def afterAll() = {
    client.performRequest(new Request("DELETE", "/_all"))
    clientV7.performRequest(new Request("DELETE", "/_all"))
    client.close()
    clientV7.close()
    TestKit.shutdownActorSystem(system)
  }

  "Connector with ApiVersion 5 running against Elasticsearch v6.8.0" should {
    behave like elasticsearchConnector(ApiVersion.V5, client)
  }

  "Connector with ApiVersion 7 running against Elasticsearch v7.6.0" should {
    behave like elasticsearchConnector(ApiVersion.V7, clientV7)
  }

}
