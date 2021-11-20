/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import io.pravega.client.ClientConfig
import io.pravega.client.admin.ReaderGroupManager
import io.pravega.client.stream.ReaderGroupConfig.ReaderGroupConfigBuilder
import io.pravega.client.stream.{ReaderGroup, ReaderGroupConfig, Stream => PravegaStream}

import scala.annotation.varargs

final class PravegaReaderGroupManager(scope: String,
                                      clientConfig: ClientConfig,
                                      readerGroupConfigBuilder: ReaderGroupConfigBuilder)
    extends AutoCloseable {

  def this(scope: String, clientConfig: ClientConfig) =
    this(scope,
         clientConfig,
         ReaderGroupConfig
           .builder())

  private lazy val readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)

  @varargs def createReaderGroup[A](groupName: String, streamNames: String*): ReaderGroup = {

    streamNames
      .map(PravegaStream.of(scope, _))
      .foreach(readerGroupConfigBuilder.stream)

    readerGroupManager.createReaderGroup(groupName, readerGroupConfigBuilder.build())
    readerGroupManager.getReaderGroup(groupName)
  }

  override def close(): Unit = readerGroupManager.close()
}
