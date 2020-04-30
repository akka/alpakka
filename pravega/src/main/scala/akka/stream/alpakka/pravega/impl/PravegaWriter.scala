/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.pravega.WriterSettings
import akka.stream.stage.StageLogging
@InternalApi private[pravega] trait PravegaWriter extends PravegaCapabilities {
  this: StageLogging =>

  def createWriter[A](streamName: String, writerSettings: WriterSettings[A]) =
    eventStreamClientFactory.createEventWriter(
      streamName,
      writerSettings.serializer,
      writerSettings.eventWriterConfig
    )

}
