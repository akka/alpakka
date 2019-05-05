package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.influxdb.scaladsl.InfluxDBSource
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import org.influxdb.InfluxDB
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import docs.javadsl.TestUtils._
import docs.javadsl.TestConstants._
import org.influxdb.dto.Query

class InfluxDBSourceSpec extends WordSpec with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll with ScalaFutures {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  implicit var influxDB: InfluxDB = _

  override protected def beforeAll(): Unit = {
    influxDB = setupConnection()
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  override def beforeEach(): Unit = {
    populateDatabase(influxDB)
  }

  override def afterEach() = {
    cleanDatabase(influxDB)
  }

  "support source" in assertAllStagesStopped {
      // #run-typed
    val query = new Query("SELECT*FROM cpu", DATABASE_NAME);

    val influxDBResult = InfluxDBSource(influxDB ,query).runWith(Sink.seq)
    val resultToAssert = influxDBResult.futureValue.head

    val values = resultToAssert.getResults.get(0).getSeries().get(0).getValues

    values.size() mustBe 2
  }

}
