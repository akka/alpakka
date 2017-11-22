/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.concurrent.{Executors, TimeoutException}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import org.mockito.Mockito.reset
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.duration._
import scala.concurrent.{blocking, Await, ExecutionContext}

trait DefaultTestContext extends BeforeAndAfterAll with BeforeAndAfterEach with MockitoSugar { this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem()
  implicit protected val materializer: Materializer = ActorMaterializer()
  private val threadPool = Executors.newFixedThreadPool(10)
  implicit protected val executionContext = ExecutionContext.fromExecutor(threadPool)

  implicit protected val amazonKinesisAsync: AmazonKinesisAsync = mock[AmazonKinesisAsync]

  override protected def beforeEach(): Unit =
    reset(amazonKinesisAsync)

  override protected def afterAll(): Unit = {
    Await.ready(system.terminate(), 5.seconds)
    threadPool.shutdown()
    if (!blocking(threadPool.awaitTermination(5, SECONDS))) throw new TimeoutException()
  }

}
