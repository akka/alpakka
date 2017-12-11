/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import scala.collection.JavaConverters._
import java.util
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

final case class JmsReplyTo(jmsDestination: Destination) extends JmsHeader {
  override val usedDuringSend = false
}
final case class JmsType(jmsType: String) extends JmsHeader {
  override val usedDuringSend = false
}

final case class JmsTimeToLive(timeInMillis: Long) extends JmsHeader {
  override val usedDuringSend = true
}

/**
 * Priority of a message can be between 0 (lowest) and 9 (highest). The default priority is 4.
 */
final case class JmsPriority(priority: Int) extends JmsHeader {
  override val usedDuringSend = true
}

/**
 * Delivery mode can be [[javax.jms.DeliveryMode.NON_PERSISTENT]] or [[javax.jms.DeliveryMode.PERSISTENT]]
 */
final case class JmsDeliveryMode(deliveryMode: Int) extends JmsHeader {
  override val usedDuringSend = true
}

object JmsCorrelationId {

  /**
   * Java API: create  [[JmsCorrelationId]]
   */
  def create(correlationId: String) = JmsCorrelationId(correlationId)
}

object JmsReplyTo {

  /**
   * Java API: create  [[JmsReplyTo]]
   */
  def queue(name: String) = JmsReplyTo(Queue(name))

  /**
   * Java API: create  [[JmsReplyTo]]
   */
  def topic(name: String) = JmsReplyTo(Topic(name))
}

object JmsType {

  /**
   * Java API: create  [[JmsType]]
   */
  def create(jmsType: String) = JmsType(jmsType)
}

object JmsTimeToLive {

  /**
   * Scala API: create [[JmsTimeToLive]]
   */
  def apply(timeToLive: Duration): JmsTimeToLive = JmsTimeToLive(timeToLive.toMillis)

  /**
   * Java API: create  [[JmsTimeToLive]]
   */
  def create(timeToLive: Long, unit: TimeUnit) = JmsTimeToLive(unit.toMillis(timeToLive))
}

object JmsPriority {

  /**
   * Java API: create  [[JmsPriority]]
   */
  def create(priority: Int) = JmsPriority(priority)
}

object JmsDeliveryMode {

  /**
   * Java API: create  [[JmsDeliveryMode]]
   */
  def create(deliveryMode: Int) = JmsDeliveryMode(deliveryMode)
}

sealed trait JmsMessage {

  def properties(): Map[String, Any]

  def headers: Set[JmsHeader]

  /**
   * Java API: adds a Jms header e.g. JMSType [[JmsMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsMessage

  /**
   * Java API: adds JMSProperty [[JmsMessage]]
   */
  def withProperty(name: String, value: Any): JmsMessage
}

final case class JmsByteMessage(bytes: Array[Byte],
                                headers: Set[JmsHeader] = Set.empty,
                                properties: Map[String, Any] = Map.empty)
    extends JmsMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType [[JmsTextMessage]]
   */
  def withHeader(jmsHeader: JmsHeader) = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty [[JmsTextMessage]]
   */
  def withProperty(name: String, value: Any) = copy(properties = properties + (name -> value))
}

object JmsByteMessage {

  /**
   * Java API: create  [[JmsByteMessage]]
   */
  def create(bytes: Array[Byte]) = JmsByteMessage(bytes = bytes)

  /**
   * Java API: create  [[JmsByteMessage]] with headers
   */
  def create(bytes: Array[Byte], headers: util.Set[JmsHeader]) =
    JmsByteMessage(bytes = bytes, headers = headers.asScala.toSet)

  /**
   * Java API: create  [[JmsByteMessage]] with properties
   */
  def create(bytes: Array[Byte], properties: util.Map[String, Any]) =
    JmsByteMessage(bytes = bytes, properties = properties.asScala.toMap)

  /**
   * Java API: create  [[JmsByteMessage]] with headers and properties
   */
  def create(bytes: Array[Byte], headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsByteMessage(bytes = bytes, headers = headers.asScala.toSet, properties = properties.asScala.toMap)
}

final case class JmsMapMessage(body: Map[String, Any],
                               headers: Set[JmsHeader] = Set.empty,
                               properties: Map[String, Any] = Map.empty)
    extends JmsMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType [[JmsMapMessage]]
   */
  def withHeader(jmsHeader: JmsHeader) = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty [[JmsMapMessage]]
   */
  def withProperty(name: String, value: Any) = copy(properties = properties + (name -> value))
}

object JmsMapMessage {

  /**
   * Java API: create  [[JmsMapMessage]]
   */
  def create(map: util.Map[String, Any]) = JmsMapMessage(body = map.asScala.toMap)

  /**
   * Java API: create  [[JmsMapMessage]] with header
   */
  def create(map: util.Map[String, Any], headers: util.Set[JmsHeader]) =
    JmsMapMessage(body = map.asScala.toMap, headers = headers.asScala.toSet)

  /**
   * Java API: create  [[JmsMapMessage]] with properties
   */
  def create(map: util.Map[String, Any], properties: util.Map[String, Any]) =
    JmsMapMessage(body = map.asScala.toMap, properties = properties.asScala.toMap)

  /**
   * Java API: create  [[JmsMapMessage]] with header and properties
   */
  def create(map: util.Map[String, Any], headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsMapMessage(body = map.asScala.toMap, headers = headers.asScala.toSet, properties = properties.asScala.toMap)

}

final case class JmsTextMessage(body: String,
                                headers: Set[JmsHeader] = Set.empty,
                                properties: Map[String, Any] = Map.empty)
    extends JmsMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType [[JmsTextMessage]]
   */
  def withHeader(jmsHeader: JmsHeader) = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty [[JmsTextMessage]]
   */
  def withProperty(name: String, value: Any) = copy(properties = properties + (name -> value))

  /**
   * Java API: add property [[JmsTextMessage]]
   */
  @deprecated("Unclear method name, use withProperty instead")
  def add(name: String, value: Any) = withProperty(name, value)
}

object JmsTextMessage {

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String) = JmsTextMessage(body = body)

  /**
   * Java API: create  [[JmsTextMessage]] with headers
   */
  def create(body: String, headers: util.Set[JmsHeader]) =
    JmsTextMessage(body = body, headers = headers.asScala.toSet)

  /**
   * Java API: create [[JmsTextMessage]] with properties
   */
  def create(body: String, properties: util.Map[String, Any]) =
    JmsTextMessage(body = body, properties = properties.asScala.toMap)

  /**
   * Java API: create [[JmsTextMessage]] with headers and properties
   */
  def create(body: String, headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsTextMessage(body = body, headers = headers.asScala.toSet, properties = properties.asScala.toMap)
}

final case class JmsObjectMessage(serializable: java.io.Serializable,
                                  headers: Set[JmsHeader] = Set.empty,
                                  properties: Map[String, Any] = Map.empty)
    extends JmsMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType [[JmsObjectMessage]]
   */
  def withHeader(jmsHeader: JmsHeader) = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty [[JmsObjectMessage]]
   */
  def withProperty(name: String, value: Any) = copy(properties = properties + (name -> value))
}

object JmsObjectMessage {

  /**
   * Java API: create [[JmsObjectMessage]]
   */
  def create(serializable: Serializable) = JmsObjectMessage(serializable = serializable)

  /**
   * Java API: create  [[JmsObjectMessage]] with header
   */
  def create(serializable: Serializable, headers: util.Set[JmsHeader]) =
    JmsObjectMessage(serializable = serializable, headers = headers.asScala.toSet)

  /**
   * Java API: create  [[JmsObjectMessage]] with properties
   */
  def create(serializable: Serializable, properties: util.Map[String, Any]) =
    JmsObjectMessage(serializable = serializable, properties = properties.asScala.toMap)

  /**
   * Java API: create  [[JmsObjectMessage]] with properties and header
   */
  def create(serializable: Serializable, headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsObjectMessage(serializable = serializable,
                     headers = headers.asScala.toSet,
                     properties = properties.asScala.toMap)

}
