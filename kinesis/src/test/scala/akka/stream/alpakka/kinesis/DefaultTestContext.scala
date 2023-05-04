/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.concurrent.{Executors, TimeoutException}

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.duration._
import scala.concurrent.{blocking, Await, ExecutionContext, ExecutionContextExecutor}

trait DefaultTestContext extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem(
    "KinesisTests",
    ConfigFactory.parseString("""
    akka.stream.materializer.initial-input-buffer-size = 1
    akka.stream.materializer.max-input-buffer-size = 1
  """)
  )
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
