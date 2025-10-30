/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.ironmq.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.ironmq.Message

/**
 * Commit an offset that is included in a [[CommittableMessage]].
 */
trait Committable {
  def commit(): CompletionStage[Done]
}

/**
 * A [[Committable]] wrapper around the IronMq [[Message]].
 */
trait CommittableMessage extends Committable {
  def message: Message
}
