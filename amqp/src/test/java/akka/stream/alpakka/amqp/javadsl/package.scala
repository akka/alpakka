/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.amqp.scaladsl.{CommittableIncomingMessage => ScalaCommittableIncomingMessage}

import scala.compat.java8.FutureConverters

package object javadsl {
  import FutureConverters._

  private[javadsl] implicit class RichCommittableIncomingMessage(cm: ScalaCommittableIncomingMessage) {
    def asJava: CommittableIncomingMessage = new CommittableIncomingMessage {
      override val message: IncomingMessage = cm.message
      override def ack(multiple: Boolean = false): CompletionStage[Done] = cm.ack(multiple).toJava
      override def nack(multiple: Boolean = false, requeue: Boolean = true): CompletionStage[Done] =
        cm.nack(multiple, requeue).toJava
    }
  }
}
