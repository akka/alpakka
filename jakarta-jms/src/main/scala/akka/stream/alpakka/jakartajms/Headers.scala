/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

sealed trait JmsHeader {

  /**
   * Indicates if this header must be set during the send() operation according to the JMS specification or as attribute of the jms message before.
   */
  def usedDuringSend: Boolean
}

final case class JmsCorrelationId(jmsCorrelationId: String) extends JmsHeader {
  override val usedDuringSend = false
}

object JmsCorrelationId {

  /**
   * Java API: create [[JmsCorrelationId]]
   */
  def create(correlationId: String) = JmsCorrelationId(correlationId)
}

final case class JmsReplyTo(jmsDestination: Destination) extends JmsHeader {
  override val usedDuringSend = false
}

object JmsReplyTo {

  /**
   * Reply to a queue with given name.
   */
  def queue(name: String) = JmsReplyTo(Queue(name))

  /**
   * Reply to a topic with given name.
   */
  def topic(name: String) = JmsReplyTo(Topic(name))
}

final case class JmsType(jmsType: String) extends JmsHeader {
  override val usedDuringSend = false
}

object JmsType {

  /**
   * Java API: create [[JmsType]]
   */
  def create(jmsType: String) = JmsType(jmsType)
}

final case class JmsTimeToLive(timeInMillis: Long) extends JmsHeader {
  override val usedDuringSend = true
}

object JmsTimeToLive {

  /**
   * Scala API: create [[JmsTimeToLive]]
   */
  def apply(timeToLive: Duration): JmsTimeToLive = JmsTimeToLive(timeToLive.toMillis)

  /**
   * Java API: create [[JmsTimeToLive]]
   */
  def create(timeToLive: Long, unit: TimeUnit) = JmsTimeToLive(unit.toMillis(timeToLive))
}

/**
 * Priority of a message can be between 0 (lowest) and 9 (highest). The default priority is 4.
 */
final case class JmsPriority(priority: Int) extends JmsHeader {
  override val usedDuringSend = true
}

object JmsPriority {

  /**
   * Java API: create [[JmsPriority]]
   */
  def create(priority: Int) = JmsPriority(priority)
}

/**
 * Delivery mode can be [[jakarta.jms.DeliveryMode.NON_PERSISTENT]] or [[jakarta.jms.DeliveryMode.PERSISTENT]]
 */
final case class JmsDeliveryMode(deliveryMode: Int) extends JmsHeader {
  override val usedDuringSend = true
}

object JmsDeliveryMode {

  /**
   * Java API: create [[JmsDeliveryMode]]
   */
  def create(deliveryMode: Int) = JmsDeliveryMode(deliveryMode)
}

final case class JmsMessageId(jmsMessageId: String) extends JmsHeader {
  override val usedDuringSend: Boolean = false
}

object JmsMessageId {

  /**
   * Java API: create [[JmsMessageId]]
   */
  def create(messageId: String) = JmsMessageId(messageId)
}

final case class JmsTimestamp(jmsTimestamp: Long) extends JmsHeader {
  override val usedDuringSend: Boolean = false
}

object JmsTimestamp {

  /**
   * Java API: create [[JmsTimestamp]]
   */
  def create(timestamp: Long) = JmsTimestamp(timestamp)
}

final case class JmsRedelivered(jmsRedelivered: Boolean) extends JmsHeader {
  override val usedDuringSend: Boolean = false
}

object JmsRedelivered {

  /**
   * Java API: create [[JmsRedelivered]]
   */
  def create(redelivered: Boolean) = JmsRedelivered(redelivered)
}

final case class JmsExpiration(jmsExpiration: Long) extends JmsHeader {
  override val usedDuringSend: Boolean = false
}

object JmsExpiration {

  /**
   * Java API: create [[JmsExpiration]]
   */
  def create(expiration: Long) = JmsExpiration(expiration)
}
