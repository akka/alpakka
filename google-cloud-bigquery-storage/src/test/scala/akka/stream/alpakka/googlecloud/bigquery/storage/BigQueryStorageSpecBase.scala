/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.bigquery.storage.mock.{BigQueryMockData, BigQueryMockServer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.Promise
import scala.concurrent.duration._

abstract class BigQueryStorageSpecBase
    extends AnyWordSpec
    with BigQueryMockData
    with ScalaFutures
    with BeforeAndAfterAll {
  self: Suite =>

  private[bigquery] val bqHost = "localhost"
  private[bigquery] val bqPort = 21000

  implicit val system: ActorSystem = ActorSystem("alpakka-bigquery-storage")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = 15.seconds, interval = 50.millis)

  private val binding = Promise[Http.ServerBinding]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val bindingRes = new BigQueryMockServer(bqPort).run().futureValue
    binding.success(bindingRes)
  }

  override protected def afterAll(): Unit = {
    binding.future.futureValue.unbind().futureValue
    super.afterAll()
  }
}
