/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.pravega

import akka.actor.ActorSystem
import akka.testkit.TestKit

import java.net.URI
import java.util.UUID
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.{ScalingPolicy, StreamConfiguration}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory
import io.pravega.client.ClientConfig
import io.pravega.client.tables.KeyValueTableConfiguration
import io.pravega.client.admin.KeyValueTableManager

abstract class PravegaBaseSpec
    extends TestKit(ActorSystem("PravegaSpec"))
    with AnyWordSpecLike
    with ScalaFutures
    with Matchers {
  val logger = LoggerFactory.getLogger(this.getClass())

  def time[R](label: String, block: => R): R = {
    val t0 = System.nanoTime() / 1000000
    val result = block
    val t1 = System.nanoTime() / 1000000
    logger.info(s"$label took " + (t1 - t0) + "ms")
    result
  }

  def newGroupName() = "scala-test-group-" + UUID.randomUUID().toString
  def newScope() = "scala-test-scope-" + UUID.randomUUID().toString

  def newKeyValueTableName() = "scala-test-kv-table" + UUID.randomUUID().toString

  def createStream(scope: String, streamName: String) = {
    val streamManager = StreamManager.create(URI.create("tcp://localhost:9090"))
    if (streamManager.createScope(scope))
      logger.info(s"Created scope [$scope].")
    else
      logger.info(s"Scope [$scope] already exists.")
    val streamConfig =
      StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(1)).build
    if (streamManager.createStream(scope, streamName, streamConfig))
      logger.info(s"Created stream [$streamName] in scope [$scope].")
    else
      logger.info(s"Stream [$streamName] already exists in scope [$scope].")

    streamManager.close()
  }

  def createTable(scope: String, tableName: String, primaryKeyLength: Int): Unit = {
    val streamManager = StreamManager.create(URI.create("tcp://localhost:9090"))
    if (streamManager.createScope(scope))
      logger.info(s"Created scope [$scope].")
    else {
      logger.info(s"Scope [$scope] already exists.")
    }
    streamManager.close()
    val clientConfig = ClientConfig
      .builder()
      .build()

    val keyValueTableConfig = KeyValueTableConfiguration
      .builder()
      .partitionCount(2)
      .primaryKeyLength(primaryKeyLength)
      .build()
    val keyValueTableManager = KeyValueTableManager.create(clientConfig)

    if (keyValueTableManager.createKeyValueTable(scope, tableName, keyValueTableConfig))
      logger.info(s"Created KeyValue table [$tableName] in scope [$scope]")
    else
      logger.info(s"KeyValue table [$tableName] already exists in scope [$scope]")

    keyValueTableManager.close()
  }
}
