/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.persistence

import akka.actor.{ ActorContext, ActorRef }
import akka.persistence.query.{ EventEnvelope, EventEnvelope2 }
import akka.persistence.{ AtomicWrite, PersistentRepr }

import scala.collection.immutable.{ Map, Seq }
import scala.reflect.ClassTag

package object writer {
  val unsupported: Throwable = new IllegalArgumentException("JournalWriter only accepts EventEnvelope, immutable.Seq[EventEnvelope], EventEnvelope2 and immutable.Seq[EventEnvelope2]")

  def replyWithFailure(replyTo: ActorRef, cause: Throwable = unsupported)(implicit ctx: ActorContext, self: ActorRef): Unit = {
    replyTo ! akka.actor.Status.Failure(cause)
    ctx.stop(self)
  }

  def replyWithSuccess(replyTo: ActorRef, msg: String = "ack")(implicit ctx: ActorContext, self: ActorRef): Unit = {
    replyTo ! msg
    ctx.stop(self)
  }

  implicit class SeqOps(val that: Seq[_]) extends AnyVal {
    def is[A](implicit ct: ClassTag[A]): Boolean =
      that.nonEmpty && that.forall(_.getClass == ct.runtimeClass)
    def as[A: ClassTag]: Seq[A] = that.map(_.asInstanceOf[A])
  }

  def toPersistentRepr(env: EventEnvelope): PersistentRepr =
    PersistentRepr(
      payload = env.event,
      sequenceNr = env.sequenceNr,
      persistenceId = env.persistenceId
    )

  def listOfEnvelopeToRepr(xs: Seq[EventEnvelope]): Seq[PersistentRepr] =
    xs.map(toPersistentRepr)

  def listOfReprToAtomicWrite(xs: Seq[PersistentRepr]): AtomicWrite =
    AtomicWrite(xs)

  def toAtomicWrite(xs: Seq[EventEnvelope]): Seq[AtomicWrite] = {
    val groupedByPid: Map[String, AtomicWrite] =
      xs.groupBy(_.persistenceId)
        .mapValues(listOfEnvelopeToRepr)
        .mapValues(listOfReprToAtomicWrite)
    Seq(groupedByPid.values.toList: _*)
  }

  def toEventEnvelope(x: EventEnvelope2): EventEnvelope =
    EventEnvelope(0L, x.persistenceId, x.sequenceNr, x.event)
}
