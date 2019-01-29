/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.dynamodb.scaladsl._
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class ItemSpec extends TestKit(ActorSystem("ItemSpec")) with AsyncWordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  val settings = DynamoSettings(system)

  override def beforeAll(): Unit = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  "DynamoDB with external client" should {

    import ItemSpecOps._

    "1) list zero tables" in {
      DynamoDb.single(listTablesRequest).map(_.getTableNames.asScala.count(_ == tableName) shouldBe 0)
    }

    "2) create a table" in {
      DynamoDb.single(createTableRequest).map(_.getTableDescription.getTableStatus shouldBe "ACTIVE")
    }

    "3) find a new table" in {
      DynamoDb.single(listTablesRequest).map(_.getTableNames.asScala.count(_ == tableName) shouldBe 1)
    }

    "4) put an item and read it back" in {
      DynamoDb
        .single(test4PutItemRequest)
        .flatMap(_ => DynamoDb.single(getItemRequest))
        .map(_.getItem.get("data").getS shouldEqual "test4data")
    }

    "5) put two items in a batch" in {
      DynamoDb.single(batchWriteItemRequest).map(_.getUnprocessedItems.size() shouldEqual 0)
    }

    "6) query two items with page size equal to 1" in {
      DynamoDb
        .source(queryItemsRequest)
        .filterNot(_.getItems.isEmpty)
        .map(_.getItems)
        .runWith(Sink.seq)
        .map { results =>
          results.size shouldBe 2
          val Seq(a, b) = results
          a.size shouldEqual 1
          a.get(0).get(sortCol) shouldEqual N(0)
          b.size shouldEqual 1
          b.get(0).get(sortCol) shouldEqual N(1)
        }
    }

    "7) delete an item" in {
      DynamoDb
        .single(deleteItemRequest)
        .flatMap(_ => DynamoDb.single(getItemRequest))
        .map(_.getItem shouldEqual null)
    }

    // The next 3 tests are ignored as DynamoDB Local does not support transactions; they
    // succeed against a cloud instance so can be enabled once local support is available.

    "8) put two items in a transaction" ignore {
      DynamoDb.single(transactPutItemsRequest).map(_ => succeed)
    }

    "9) get two items in a transaction" ignore {
      DynamoDb.single(transactGetItemsRequest).map { results =>
        results.getResponses.size shouldBe 2
        val Seq(a, b) = results.getResponses.asScala
        a.getItem.get(sortCol) shouldEqual N(0)
        b.getItem.get(sortCol) shouldEqual N(1)
      }
    }

    "10) delete two items in a transaction" ignore {
      DynamoDb.single(transactDeleteItemsRequest).map(_ => succeed)
    }

    "11) delete table" in {
      DynamoDb
        .single(deleteTableRequest)
        .flatMap(_ => DynamoDb.single(listTablesRequest))
        .map(_.getTableNames.asScala.count(_ == tableName) shouldEqual 0)
    }

  }

}
