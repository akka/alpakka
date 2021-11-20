/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util.concurrent.atomic.AtomicBoolean

import akka.stream.alpakka.jms.impl.{JmsAckSession, JmsSession}
import javax.jms

import scala.concurrent.{Future, Promise}

case class AckEnvelope private[jms] (message: jms.Message, private val jmsSession: JmsAckSession) {

  val processed = new AtomicBoolean(false)

  def acknowledge(): Unit = if (processed.compareAndSet(false, true)) jmsSession.ack(message)
}

case class TxEnvelope private[jms] (message: jms.Message, private val jmsSession: JmsSession) {

  private[this] val commitPromise = Promise[() => Unit]()

  private[jms] val commitFuture: Future[() => Unit] = commitPromise.future

  def commit(): Unit = commitPromise.success(jmsSession.session.commit _)

  def rollback(): Unit = commitPromise.success(jmsSession.session.rollback _)
}
