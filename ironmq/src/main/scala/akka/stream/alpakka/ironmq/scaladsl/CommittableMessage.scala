/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.scaladsl

import akka.Done
import akka.stream.alpakka.ironmq.Message

import scala.concurrent.Future

/**
 * Commit an offset that is included in a [[CommittableMessage]].
 */
trait Committable {
  def commit(): Future[Done]
}

/**
 * A [[Committable]] wrapper around the IronMq [[Message]].
 */
trait CommittableMessage extends Committable {
  def message: Message
}
