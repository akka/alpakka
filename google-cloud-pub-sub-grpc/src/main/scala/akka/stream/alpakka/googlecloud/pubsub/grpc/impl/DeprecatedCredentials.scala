/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.impl

import akka.annotation.InternalApi
import io.grpc.CallCredentials

import java.util.concurrent.Executor

/**
 * Used purely as a placeholder class to help migrate to common Google auth.
 */
@InternalApi
private[grpc] object DeprecatedCredentials extends CallCredentials {
  override def applyRequestMetadata(requestInfo: CallCredentials.RequestInfo,
                                    appExecutor: Executor,
                                    applier: CallCredentials.MetadataApplier): Unit = ???
  override def thisUsesUnstableApi(): Unit = ???
}
