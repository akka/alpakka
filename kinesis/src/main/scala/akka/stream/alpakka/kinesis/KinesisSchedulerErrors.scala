/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.kinesis

object KinesisSchedulerErrors {

  sealed class KinesisSchedulerError(err: Throwable) extends Throwable(err)
  final case class SchedulerUnexpectedShutdown(cause: Throwable) extends KinesisSchedulerError(cause)

}
