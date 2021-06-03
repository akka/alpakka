/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega.impl
import akka.annotation.InternalApi
import akka.stream.stage.StageLogging
import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory

import scala.util.{Failure, Success, Try}

@InternalApi private[pravega] trait PravegaCapabilities {
  this: StageLogging =>

  protected val scope: String
  protected val clientConfig: ClientConfig

  lazy val eventStreamClientFactory = EventStreamClientFactory.withScope(scope, clientConfig)

  def close() = Try(eventStreamClientFactory.close()) match {
    case Failure(exception) =>
      log.error(exception, "Error while closing scope [{}]", scope)
    case Success(value) =>
      log.debug("Closed scope [{}]", scope)
  }

}
