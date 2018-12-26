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

class ItemSpec extends TestKit(ActorSystem("ItemSpec")) with AsyncWordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val settings = DynamoSettings(system)

  override def beforeAll() = {
    System.setProperty("aws.accessKeyId", "someKeyId")
    System.setProperty("aws.secretKey", "someSecretKey")
  }

  "DynamoDB with external client" should {

    import DynamoImplicits._
    import ItemSpecOps._

    implicit val client = DynamoClient(settings)

    "1) list zero tables" in {
      DynamoDbExternal.single(listTablesRequest).map(_.getTableNames.asScala.count(_ == tableName) shouldBe 0)
    }

    "2) create a table" in {
      DynamoDbExternal.single(createTableRequest).map(_.getTableDescription.getTableStatus shouldBe "ACTIVE")
    }

    "3) find a new table" in {
      DynamoDbExternal.single(listTablesRequest).map(_.getTableNames.asScala.count(_ == tableName) shouldBe 1)
    }

    "4) put an item and read it back" in {
      DynamoDbExternal
        .single(test4PutItemRequest)
        .flatMap(_ => DynamoDbExternal.single(getItemRequest))
        .map(_.getItem.get("data").getS shouldEqual "test4data")
    }

    "5) put two items in a batch" in {
      DynamoDbExternal.single(batchWriteItemRequest).map(_.getUnprocessedItems.size() shouldEqual 0)
    }

    "6) query two items with page size equal to 1" in {
      DynamoDbExternal
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
      DynamoDbExternal
        .single(deleteItemRequest)
        .flatMap(_ => DynamoDbExternal.single(getItemRequest))
        .map(_.getItem() shouldEqual null)
    }

    "8) delete table" in {
      DynamoDbExternal
        .single(deleteTableRequest)
        .flatMap(_ => DynamoDbExternal.single(listTablesRequest))
        .map(_.getTableNames.asScala.count(_ == tableName) shouldEqual 0)
    }

  }

}
