/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.Done

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
