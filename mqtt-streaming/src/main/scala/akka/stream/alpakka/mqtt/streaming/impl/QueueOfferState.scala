/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.impl

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.QueueOfferResult

private[mqtt] object QueueOfferState {

  /**
   * A marker trait that holds a result for SourceQueue#offer
   */
  trait QueueOfferCompleted {
    def result: Either[Throwable, QueueOfferResult]
  }

  /**
   * A behavior that stashes messages until a response to the SourceQueue#offer
   * method is received.
   *
   * This is to be used only with SourceQueues that use backpressure.
   */
  def waitForQueueOfferCompleted[T](behavior: Behavior[T], stash: Seq[T]): Behavior[T] =
    Behaviors
      .receive[T] {
        case (context, completed: QueueOfferCompleted) =>
          completed.result match {
            case Right(QueueOfferResult.Enqueued) =>
              stash.foreach(context.self.tell)

              behavior

            case Right(other) =>
              throw new IllegalStateException(s"Failed to offer to queue: $other")

            case Left(failure) =>
              throw failure
          }

        case (_, other) =>
          waitForQueueOfferCompleted(behavior, stash = stash :+ other)
      }
      .receiveSignal { case (ctx, signal) => Behavior.interpretSignal(behavior, ctx, signal) } // handle signals immediately
}
