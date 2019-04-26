/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.concurrent.{Executors, TimeoutException}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.duration._
import scala.concurrent.{blocking, Await, ExecutionContext, ExecutionContextExecutor}

trait DefaultTestContext extends BeforeAndAfterAll with BeforeAndAfterEach with MockitoSugar { this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem()
  implicit protected val materializer: Materializer = ActorMaterializer()
  private val threadPool = Executors.newFixedThreadPool(10)
  implicit protected val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(threadPool)

  override protected def afterAll(): Unit = {
    Await.ready(system.terminate(), 5.seconds)
    threadPool.shutdown()
    if (!blocking(threadPool.awaitTermination(5, SECONDS)))
      throw new TimeoutException()
  }

}
