/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
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
