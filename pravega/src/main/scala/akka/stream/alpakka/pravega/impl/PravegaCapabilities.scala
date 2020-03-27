/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl
import akka.annotation.InternalApi
import akka.stream.stage.StageLogging
import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory

@InternalApi private[pravega] trait PravegaCapabilities {
  this: StageLogging =>

  val scope: String
  val clientConfig: ClientConfig

  lazy val eventStreamClientFactory = EventStreamClientFactory.withScope(scope, clientConfig)

  def close() = eventStreamClientFactory.close()

}
