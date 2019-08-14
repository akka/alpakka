/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.AwsOp.BatchGetItem
import akka.stream.alpakka.dynamodb.ItemSpecOps
import akka.stream.alpakka.dynamodb.impl.scaladsl.RetryFlow
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Success

class RetrySpec
    extends TestKit(ActorSystem("RetrySpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import ItemSpecOps._

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 100.millis)

  override def beforeAll(): Unit = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  override def afterAll(): Unit =
    DynamoDb.single(deleteTableRequest).futureValue

  "DynamoDb connector" should {

    "retry requests" in {
      DynamoDb.single(createTableRequest).futureValue
      DynamoDb.single(batchWriteLargeItemRequest(1, 25)).futureValue
      DynamoDb.single(batchWriteLargeItemRequest(26, 50)).futureValue

      val retryFlow = RetryFlow.withBackoff(8, 10.millis, 5.seconds, 1, DynamoDb.tryFlow[BatchGetItem, Unit]) {
        case (Success(resp), _) if resp.getUnprocessedKeys.size() > 0 =>
          Some(List((batchGetItemRequest(resp.getUnprocessedKeys), ())))
        case _ => None
      }

      val responses = Source
        .single((BatchGetItem(batchGetLargeItemRequest(1, 50)), ()))
        .via(retryFlow)
        .runFold(0)((cnt, _) => cnt + 1)
        .futureValue

      responses shouldBe 2
    }

  }
}
