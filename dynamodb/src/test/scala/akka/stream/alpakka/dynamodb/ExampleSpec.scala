/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ExampleSpec extends TestKit(ActorSystem("ExampleSpec")) with WordSpecLike with Matchers with BeforeAndAfterAll {

  val settings = DynamoSettings(system)

  override def beforeAll() = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  "DynamoDB Client" should {

    "provide a simple usage example" in {

      //#init-client
      implicit val system = ActorSystem()
      implicit val materializer = ActorMaterializer()
      implicit val ec = system.dispatcher
      //#init-client

      //#client-construct
      val settings = DynamoSettings(system)
      val client = DynamoClient(settings)
      //#client-construct

      //##simple-request
      import DynamoImplicits._
      val listTablesResult: Future[ListTablesResult] = client.single(new ListTablesRequest())
      //##simple-request

      Await.result(listTablesResult, 5.seconds)

      system.terminate()

    }

  }

}
