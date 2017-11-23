/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import scala.collection.JavaConverters._
import java.util

sealed trait JmsHeader
final case class JmsCorrelationId(jmsCorrelationId: String) extends JmsHeader
final case class JmsReplyTo(jmsDestination: Destination) extends JmsHeader
final case class JmsType(jmsType: String) extends JmsHeader

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
