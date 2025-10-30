/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.impl

import akka.annotation.InternalApi
import io.grpc.CallCredentials

import java.util.concurrent.Executor

/**
 * Used purely as a wrapper class to help migrate to common Google auth.
 */
@InternalApi
private[grpc] final case class DeprecatedCredentials(underlying: CallCredentials) extends CallCredentials {

  override def applyRequestMetadata(requestInfo: CallCredentials.RequestInfo,
                                    appExecutor: Executor,
                                    applier: CallCredentials.MetadataApplier): Unit =
    underlying.applyRequestMetadata(requestInfo, appExecutor, applier)

  @deprecated("unstable API", "7.0.0")
  override def thisUsesUnstableApi(): Unit = underlying.thisUsesUnstableApi()
}
