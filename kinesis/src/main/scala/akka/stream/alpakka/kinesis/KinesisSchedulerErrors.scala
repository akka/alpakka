/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

object KinesisSchedulerErrors {

  sealed class KinesisSchedulerError(err: Throwable) extends Throwable(err)
  final case class SchedulerUnexpectedShutdown(cause: Throwable) extends KinesisSchedulerError(cause)

}
