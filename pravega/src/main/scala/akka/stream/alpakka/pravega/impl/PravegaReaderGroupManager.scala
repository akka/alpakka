/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl

import akka.stream.alpakka.pravega.PravegaReaderGroup
import io.pravega.client.ClientConfig
import io.pravega.client.admin.ReaderGroupManager
import io.pravega.client.stream.{ReaderGroupConfig, Stream => PravegaStream}

import scala.annotation.varargs
import io.pravega.client.stream.ReaderGroupConfig.ReaderGroupConfigBuilder

class PravegaReaderGroupManager(scope: String,
                                clientConfig: ClientConfig,
                                readerGroupConfigBuilder: ReaderGroupConfigBuilder)
    extends AutoCloseable {

  def this(scope: String, clientConfig: ClientConfig) =
    this(scope,
         clientConfig,
         ReaderGroupConfig
           .builder())

  private lazy val readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)

  @varargs def createReaderGroup[A](groupName: String, streamNames: String*): PravegaReaderGroup = {

    streamNames
      .map(PravegaStream.of(scope, _))
      .foreach(readerGroupConfigBuilder.stream)

    readerGroupManager.createReaderGroup(groupName, readerGroupConfigBuilder.build())
    PravegaReaderGroup(readerGroupManager.getReaderGroup(groupName), streamNames.toSet)

  }

  override def close(): Unit = readerGroupManager.close()
}
