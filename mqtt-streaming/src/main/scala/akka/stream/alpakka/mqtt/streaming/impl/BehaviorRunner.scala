/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.mqtt.streaming.impl

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{Behavior, Signal}

import scala.collection.immutable.Seq

object BehaviorRunner {
  sealed trait Interpretable[T]

  final case class StoredMessage[T](message: T) extends Interpretable[T]

  final case class StoredSignal[T](signal: Signal) extends Interpretable[T]

  /**
   * Interpreter all of the supplied messages or signals, returning
   * the resulting behavior.
   */
  def run[T](behavior: Behavior[T], context: ActorContext[T], stash: Seq[Interpretable[T]]): Behavior[T] =
    stash.foldLeft(Behavior.start(behavior, context)) {
      case (b, StoredMessage(msg)) =>
        val nextBehavior = Behavior.interpretMessage(b, context, msg)

        if ((nextBehavior ne Behaviors.same) && (nextBehavior ne Behaviors.unhandled)) {
          nextBehavior
        } else {
          b
        }

      case (b, StoredSignal(signal)) =>
        val nextBehavior = Behavior.interpretSignal(b, context, signal)

        if ((nextBehavior ne Behaviors.same) && (nextBehavior ne Behaviors.unhandled)) {
          nextBehavior
        } else {
          b
        }
    }
}
