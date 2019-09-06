/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.net.URI

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.ItemSpecOps
import akka.stream.alpakka.dynamodb.impl.scaladsl.RetryFlow
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpecLike}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemRequest,
  BatchGetItemResponse,
  GetItemRequest,
  GetItemResponse
}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class RetrySpec
    extends TestKit(ActorSystem("RetrySpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with ScalaFutures {

  import ItemSpecOps._

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  implicit val client: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .region(Region.AWS_GLOBAL)
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
    .endpointOverride(new URI("http://localhost:8001/"))
    .build()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 100.millis)

  override def beforeAll(): Unit = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  override def afterAll(): Unit =
    client.close()

  before {
    DynamoDb.single(createTableRequest).futureValue
  }

  "DynamoDb connector" should {

    "retry successful requests" in {
      DynamoDb.single(batchWriteLargeItemRequest(1, 25)).futureValue
      DynamoDb.single(batchWriteLargeItemRequest(26, 50)).futureValue

      val retryFlow =
        RetryFlow.withBackoff(8,
                              10.millis,
                              5.seconds,
                              1,
                              DynamoDb.tryFlow[BatchGetItemRequest, BatchGetItemResponse, NotUsed](1)) {
          case (Success(resp), _) if resp.unprocessedKeys.size() > 0 =>
            Some(List((batchGetItemRequest(resp.unprocessedKeys), NotUsed)))
        }

      val responses = Source
        .single((batchGetLargeItemRequest(1, 50), NotUsed))
        .via(retryFlow)
        .runFold(0)((cnt, _) => cnt + 1)
        .futureValue

      responses shouldBe 2
    }

    "retry failed requests" in {
      //#create-retry-flow
      val retryFlow =
        RetryFlow.withBackoff(8, 10.millis, 5.seconds, 1, DynamoDb.tryFlow[GetItemRequest, GetItemResponse, Int](1)) {
          case (Failure(_), retries) => Some(List((getItemRequest, retries + 1)))
        }
      //#create-retry-flow

      //#use-retry-flow
      val (response, retries) =
        Source.single((getItemMalformedRequest, 0)).via(retryFlow).runWith(Sink.head).futureValue
      //#use-retry-flow

      response shouldBe a[Success[_]]
      retries shouldBe 1
    }

    after {
      DynamoDb.single(deleteTableRequest).futureValue
    }

  }
}
