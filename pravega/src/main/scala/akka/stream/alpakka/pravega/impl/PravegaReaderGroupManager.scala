/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl

import akka.stream.alpakka.pravega.PravegaReaderGroup
import io.pravega.client.ClientConfig
import io.pravega.client.admin.ReaderGroupManager
import io.pravega.client.stream.{ReaderGroup, ReaderGroupConfig, Stream => PravegaStream}

import scala.annotation.varargs

class PravegaReaderGroupManager(scope: String, clientConfig: ClientConfig) extends AutoCloseable {

  private lazy val readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)

  @varargs def createReaderGroup[A](groupName: String, streamNames: String*): PravegaReaderGroup = {
    val configBuilder = ReaderGroupConfig
      .builder()

    streamNames
      .map(PravegaStream.of(scope, _))
      .foreach(configBuilder.stream)

    readerGroupManager.createReaderGroup(groupName, configBuilder.build())
    PravegaReaderGroup(readerGroupManager.getReaderGroup(groupName), streamNames.toSet)

  }

  override def close(): Unit = readerGroupManager.close()
}
