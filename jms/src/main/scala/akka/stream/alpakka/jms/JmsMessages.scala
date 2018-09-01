/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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
   * Java API: create [[JmsCorrelationId]]
   */
  def create(correlationId: String) = JmsCorrelationId(correlationId)
}

object JmsReplyTo {

  /**
   * Java API: create [[JmsReplyTo]]
   */
  def queue(name: String) = JmsReplyTo(Queue(name))

  /**
   * Java API: create [[JmsReplyTo]]
   */
  def topic(name: String) = JmsReplyTo(Topic(name))
}

object JmsType {

  /**
   * Java API: create [[JmsType]]
   */
  def create(jmsType: String) = JmsType(jmsType)
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

object JmsPriority {

  /**
   * Java API: create [[JmsPriority]]
   */
  def create(priority: Int) = JmsPriority(priority)
}

object JmsDeliveryMode {

  /**
   * Java API: create [[JmsDeliveryMode]]
   */
  def create(deliveryMode: Int) = JmsDeliveryMode(deliveryMode)
}

sealed trait JmsMessage {

  def properties: Map[String, Any]

  def headers: Set[JmsHeader]

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsMessage

  /**
   * Java API: adds JMSProperty to [[JmsMessage]]
   */
  def withProperty(name: String, value: Any): JmsMessage

  def toQueue(name: String): JmsDirectedMessage

  def toTopic(name: String): JmsDirectedMessage

  def to(destination: Destination): JmsDirectedMessage
}

sealed trait JmsDirectedMessage extends JmsMessage {

  val destination: Destination

  def withoutDestination: JmsMessage
}

sealed abstract class JmsAbstractByteMessage extends JmsMessage {

  def bytes: Array[Byte]

  def toQueue(name: String): JmsDirectedByteMessage = to(Queue(name))

  def toTopic(name: String): JmsDirectedByteMessage = to(Topic(name))

  def to(destination: Destination): JmsDirectedByteMessage =
    JmsDirectedByteMessage(bytes, destination, headers, properties)
}

sealed abstract class JmsAbstractMapMessage extends JmsMessage {

  def body: Map[String, Any]

  def toQueue(name: String): JmsDirectedMapMessage = to(Queue(name))

  def toTopic(name: String): JmsDirectedMapMessage = to(Topic(name))

  def to(destination: Destination): JmsDirectedMapMessage =
    JmsDirectedMapMessage(body, destination, headers, properties)
}

sealed abstract class JmsAbstractTextMessage extends JmsMessage {

  def body: String

  def toQueue(name: String): JmsDirectedTextMessage = to(Queue(name))

  def toTopic(name: String): JmsDirectedTextMessage = to(Topic(name))

  def to(destination: Destination): JmsDirectedTextMessage =
    JmsDirectedTextMessage(body, destination, headers, properties)
}

sealed abstract class JmsAbstractObjectMessage extends JmsMessage {

  def serializable: java.io.Serializable

  def toQueue(name: String): JmsDirectedObjectMessage = to(Queue(name))

  def toTopic(name: String): JmsDirectedObjectMessage = to(Topic(name))

  def to(destination: Destination): JmsDirectedObjectMessage =
    JmsDirectedObjectMessage(serializable, destination, headers, properties)

}

final case class JmsByteMessage(bytes: Array[Byte],
                                headers: Set[JmsHeader] = Set.empty,
                                properties: Map[String, Any] = Map.empty)
    extends JmsAbstractByteMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsByteMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsByteMessage = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty to [[JmsByteMessage]]
   */
  def withProperty(name: String, value: Any): JmsByteMessage = copy(properties = properties + (name -> value))
}

final case class JmsDirectedByteMessage(bytes: Array[Byte],
                                        destination: Destination,
                                        headers: Set[JmsHeader] = Set.empty,
                                        properties: Map[String, Any] = Map.empty)
    extends JmsAbstractByteMessage
    with JmsDirectedMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsByteMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsDirectedByteMessage = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty to [[JmsByteMessage]]
   */
  def withProperty(name: String, value: Any): JmsDirectedByteMessage = copy(properties = properties + (name -> value))

  def withoutDestination: JmsByteMessage = JmsByteMessage(bytes, headers, properties)
}

object JmsByteMessage {

  /**
   * Java API: create [[JmsByteMessage]]
   */
  def create(bytes: Array[Byte]) = JmsByteMessage(bytes = bytes)

  /**
   * Java API: create [[JmsByteMessage]] with headers
   */
  def create(bytes: Array[Byte], headers: util.Set[JmsHeader]) =
    JmsByteMessage(bytes = bytes, headers = headers.asScala.toSet)

  /**
   * Java API: create [[JmsByteMessage]] with properties
   */
  def create(bytes: Array[Byte], properties: util.Map[String, Any]) =
    JmsByteMessage(bytes = bytes, properties = properties.asScala.toMap)

  /**
   * Java API: create [[JmsByteMessage]] with headers and properties
   */
  def create(bytes: Array[Byte], headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsByteMessage(bytes = bytes, headers = headers.asScala.toSet, properties = properties.asScala.toMap)
}

final case class JmsMapMessage(body: Map[String, Any],
                               headers: Set[JmsHeader] = Set.empty,
                               properties: Map[String, Any] = Map.empty)
    extends JmsAbstractMapMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsMapMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsMapMessage = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty to [[JmsMapMessage]]
   */
  def withProperty(name: String, value: Any): JmsMapMessage = copy(properties = properties + (name -> value))

}

final case class JmsDirectedMapMessage(body: Map[String, Any],
                                       destination: Destination,
                                       headers: Set[JmsHeader] = Set.empty,
                                       properties: Map[String, Any] = Map.empty)
    extends JmsAbstractMapMessage
    with JmsDirectedMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsMapMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsDirectedMapMessage = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty to [[JmsMapMessage]]
   */
  def withProperty(name: String, value: Any): JmsDirectedMapMessage = copy(properties = properties + (name -> value))

  def withoutDestination: JmsMapMessage = JmsMapMessage(body, headers, properties)
}

object JmsMapMessage {

  /**
   * Java API: create [[JmsMapMessage]]
   */
  def create(map: util.Map[String, Any]) = JmsMapMessage(body = map.asScala.toMap)

  /**
   * Java API: create [[JmsMapMessage]] with header
   */
  def create(map: util.Map[String, Any], headers: util.Set[JmsHeader]) =
    JmsMapMessage(body = map.asScala.toMap, headers = headers.asScala.toSet)

  /**
   * Java API: create [[JmsMapMessage]] with properties
   */
  def create(map: util.Map[String, Any], properties: util.Map[String, Any]) =
    JmsMapMessage(body = map.asScala.toMap, properties = properties.asScala.toMap)

  /**
   * Java API: create [[JmsMapMessage]] with header and properties
   */
  def create(map: util.Map[String, Any], headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsMapMessage(body = map.asScala.toMap, headers = headers.asScala.toSet, properties = properties.asScala.toMap)

}

final case class JmsTextMessage(body: String,
                                headers: Set[JmsHeader] = Set.empty,
                                properties: Map[String, Any] = Map.empty)
    extends JmsAbstractTextMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsTextMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsTextMessage = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty to [[JmsTextMessage]]
   */
  def withProperty(name: String, value: Any): JmsTextMessage = copy(properties = properties + (name -> value))

  /**
   * Java API: add property to [[JmsTextMessage]]
   */
  @deprecated("Unclear method name, use withProperty instead", "0.15")
  def add(name: String, value: Any): JmsTextMessage = withProperty(name, value)
}

final case class JmsDirectedTextMessage(body: String,
                                        destination: Destination,
                                        headers: Set[JmsHeader] = Set.empty,
                                        properties: Map[String, Any] = Map.empty)
    extends JmsAbstractTextMessage
    with JmsDirectedMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsDirectedTextMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsDirectedTextMessage = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty to [[JmsDirectedTextMessage]]
   */
  def withProperty(name: String, value: Any): JmsDirectedTextMessage = copy(properties = properties + (name -> value))

  def withoutDestination: JmsTextMessage = JmsTextMessage(body, headers, properties)
}

object JmsTextMessage {

  /**
   * Java API: create [[JmsTextMessage]]
   */
  def create(body: String) = JmsTextMessage(body = body)

  /**
   * Java API: create [[JmsTextMessage]] with headers
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
    extends JmsAbstractObjectMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsObjectMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsObjectMessage = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty to [[JmsObjectMessage]]
   */
  def withProperty(name: String, value: Any): JmsObjectMessage = copy(properties = properties + (name -> value))
}

final case class JmsDirectedObjectMessage(serializable: java.io.Serializable,
                                          destination: Destination,
                                          headers: Set[JmsHeader] = Set.empty,
                                          properties: Map[String, Any] = Map.empty)
    extends JmsAbstractObjectMessage
    with JmsDirectedMessage {

  /**
   * Java API: adds a Jms header e.g. JMSType to [[JmsDirectedObjectMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsDirectedObjectMessage = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty to [[JmsDirectedObjectMessage]]
   */
  def withProperty(name: String, value: Any): JmsDirectedObjectMessage = copy(properties = properties + (name -> value))

  def withoutDestination: JmsObjectMessage = JmsObjectMessage(serializable, headers, properties)
}

object JmsObjectMessage {

  /**
   * Java API: create [[JmsObjectMessage]]
   */
  def create(serializable: Serializable) = JmsObjectMessage(serializable = serializable)

  /**
   * Java API: create [[JmsObjectMessage]] with header
   */
  def create(serializable: Serializable, headers: util.Set[JmsHeader]) =
    JmsObjectMessage(serializable = serializable, headers = headers.asScala.toSet)

  /**
   * Java API: create [[JmsObjectMessage]] with properties
   */
  def create(serializable: Serializable, properties: util.Map[String, Any]) =
    JmsObjectMessage(serializable = serializable, properties = properties.asScala.toMap)

  /**
   * Java API: create [[JmsObjectMessage]] with properties and header
   */
  def create(serializable: Serializable, headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsObjectMessage(serializable = serializable,
                     headers = headers.asScala.toSet,
                     properties = properties.asScala.toMap)
}
