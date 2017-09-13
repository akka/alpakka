/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ibm.eventstore.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.ibm.eventstore.EventStoreConfiguration
import akka.stream.scaladsl._
import akka.testkit.TestKit
import com.ibm.event.catalog.TableSchema
import com.ibm.event.common.ConfigurationReader
import com.ibm.event.oltp.EventContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

/**
 * This unit test is run using a local installation of EventStore
 * The installer for EventStore can be obtained from:
 * https://www.ibm.com/us-en/marketplace/project-eventstore
 */
@Ignore
class EventStoreSpec
    extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MustMatchers {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = mat.executionContext

  implicit val defaultPatience = PatienceConfig(timeout = 3.seconds, interval = 50.millis)

  // #configuration
  val configuration = EventStoreConfiguration(ConfigFactory.load())
  // #configuration

  // #configure-endpoint
  ConfigurationReader.setConnectionEndpoints(configuration.endpoint)
  // #configure-endpoint

  override def beforeAll(): Unit = {

    EventContext.dropDatabase(configuration.databaseName)
    val context = EventContext.createDatabase("TESTDB")

    val reviewSchema = TableSchema(
      configuration.tableName,
      StructType(
        Array(
          StructField("id", LongType, nullable = false),
          StructField("someInt", IntegerType, nullable = false),
          StructField("someString", StringType, nullable = false),
          StructField("someBoolean", BooleanType, nullable = false),
          StructField("someOtherBoolean", BooleanType, nullable = true)
        )
      ),
      shardingColumns = Seq("id"),
      pkColumns = Seq("id")
    )
    context.createTable(reviewSchema)

  }

  override def afterAll(): Unit = {
    EventContext.dropDatabase(configuration.databaseName)
    // #cleanup
    EventContext.cleanUp()
    // #cleanup
    TestKit.shutdownActorSystem(system)
  }

  "insert 3 rows into EventStore" in {

    //#insert-rows
    val rows =
      List(
        Row(1L, 1, "Hello", true, false),
        Row(2L, 2, "Hello", false, null),
        Row(3L, 3, "Hello", true, true)
      )

    val future = Source(rows).runWith(EventStoreSink(configuration))
    //#insert-rows

    future.futureValue mustBe Done

  }
}
