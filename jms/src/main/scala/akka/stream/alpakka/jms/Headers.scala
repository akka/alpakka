/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms
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
 * Delivery mode can be [[javax.jms.DeliveryMode.NON_PERSISTENT]] or [[javax.jms.DeliveryMode.PERSISTENT]]
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
