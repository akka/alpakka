/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import java.net.URI
import java.util.UUID

import akka.stream.{ActorMaterializer, Materializer}

import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.{ScalingPolicy, StreamConfiguration}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

abstract class PravegaBaseSpec extends AnyWordSpec with PravegaAkkaSpecSupport with ScalaFutures with Matchers {
  val logger = LoggerFactory.getLogger(this.getClass())

  implicit val mat: Materializer = ActorMaterializer()

  final val groupName = "scala-test-group"

  final val scope = "scala-test-scope-" + UUID.randomUUID().toString
  final val streamName = "scala-test-stream-" + UUID.randomUUID().toString

  override def beforeAll: Unit = {
    val streamManager = StreamManager.create(URI.create("tcp://localhost:9090"))
    if (streamManager.createScope(scope))
      logger.info(s"Created scope [$scope].")
    else
      logger.info(s"Scope [$scope] already exists.")
    val streamConfig =
      StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(10)).build
    if (streamManager.createStream(scope, streamName, streamConfig))
      logger.info(s"Created stream [$streamName] in scope [$scope].")
    else
      logger.info(s"Stream [$streamName] already exists in scope [$scope].")

    streamManager.close()
  }

}
