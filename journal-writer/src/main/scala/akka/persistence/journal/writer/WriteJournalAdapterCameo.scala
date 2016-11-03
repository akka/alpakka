/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.persistence.journal.writer

import akka.actor.{ Actor, ActorRef }
import akka.persistence.JournalProtocol._
import akka.persistence.query.EventEnvelope
import akka.stream.alpakka.persistence.writer._

import scala.collection.immutable.Seq

object WriteJournalAdapterCameo {
  def writeMessages(xs: Seq[EventEnvelope], writerJournalAdapter: ActorRef): WriteMessages =
    WriteMessages(toAtomicWrite(xs), writerJournalAdapter, 1)
}

class WriteJournalAdapterCameo(writePlugin: ActorRef, replyTo: ActorRef, messages: Seq[EventEnvelope]) extends Actor {
  import WriteJournalAdapterCameo._
  override def preStart(): Unit = {
    if (messages.isEmpty)
      replyWithSuccess(replyTo)
    else
      writePlugin ! writeMessages(messages, self)
  }

  override def receive: Receive = {
    case msg: WriteMessageSuccess =>
      replyWithSuccess(replyTo)
    case WriteMessagesSuccessful =>
      replyWithSuccess(replyTo)
    case WriteMessagesFailed(cause) =>
      replyWithFailure(replyTo, cause)
    case WriteMessageRejected(_, cause, _) =>
      replyWithFailure(replyTo, cause)
    case WriteMessageFailure(_, cause, _) =>
      replyWithFailure(replyTo, cause)
  }
}
