/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl

import java.util.UUID

import akka.annotation.InternalApi
import akka.stream.alpakka.pravega.ReaderSettings
import akka.stream.stage.StageLogging
import io.pravega.client.admin.ReaderGroupManager
import io.pravega.client.stream.{ReaderGroup, ReaderGroupConfig, StreamCut, Stream => PravegaStream}

@InternalApi private[pravega] trait PravegaReader extends PravegaCapabilities {
  this: StageLogging =>

  private lazy val readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)

  def createReader[A](settings: ReaderSettings[A],
                      streamName: String,
                      start: StreamCut = StreamCut.UNBOUNDED,
                      end: StreamCut = StreamCut.UNBOUNDED) = {
    val readerGroup = createReaderGroup(settings, streamName)
    val eventStreamReader = eventStreamClientFactory.createReader(
      settings.readerId.getOrElse(UUID.randomUUID().toString),
      settings.groupName,
      settings.serializer,
      settings.readerConfig
    )
    new Reader(readerGroup, eventStreamReader)
  }

  private def createReaderGroup[A](readerSettings: ReaderSettings[A],
                                   streamName: String,
                                   start: StreamCut = StreamCut.UNBOUNDED,
                                   end: StreamCut = StreamCut.UNBOUNDED): ReaderGroup = {
    val config = ReaderGroupConfig
      .builder()
      .stream(PravegaStream.of(scope, streamName))
      .build()

    readerGroupManager.createReaderGroup(readerSettings.groupName, config)
    readerGroupManager.getReaderGroup(readerSettings.groupName)

  }

  override def close() = {
    readerGroupManager.close()
    super.close()
  }

}
