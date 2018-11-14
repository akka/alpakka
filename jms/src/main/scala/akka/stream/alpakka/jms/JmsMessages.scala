/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util

import akka.util.ByteString

import scala.collection.JavaConverters._

sealed trait JmsMessage {

  def properties: Map[String, Any]

  def headers: Set[JmsHeader]

  def destination: Option[Destination]

  /**
   * Add a Jms header e.g. JMSType to [[JmsMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsMessage

  /**
   * Add a property to [[JmsMessage]]
   */
  def withProperty(name: String, value: Any): JmsMessage

  def toQueue(name: String): JmsMessage

  def toTopic(name: String): JmsMessage

  def to(destination: Destination): JmsMessage

  def withoutDestination: JmsMessage
}

final case class JmsByteMessage(bytes: Array[Byte],
                                headers: Set[JmsHeader] = Set.empty,
                                properties: Map[String, Any] = Map.empty,
                                destination: Option[Destination] = None)
    extends JmsMessage {

  /**
   * Add a Jms header e.g. JMSType to [[JmsByteMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsByteMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property to [[JmsByteMessage]]
   */
  def withProperty(name: String, value: Any): JmsByteMessage = copy(properties = properties + (name -> value))

  def toQueue(name: String): JmsByteMessage = to(Queue(name))

  def toTopic(name: String): JmsByteMessage = to(Topic(name))

  def to(destination: Destination): JmsByteMessage = copy(destination = Some(destination))

  def withoutDestination: JmsByteMessage = copy(destination = None)
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

final class JmsByteStringMessage private (val bytes: ByteString,
                                          val headers: Set[JmsHeader] = Set.empty,
                                          val properties: Map[String, Any] = Map.empty,
                                          val destination: Option[Destination] = None)
    extends JmsMessage {

  /**
   * Add a Jms header e.g. JMSType to [[JmsByteStringMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsByteStringMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property to [[JmsByteMessage]]
   */
  def withProperty(name: String, value: Any): JmsByteStringMessage = copy(properties = properties + (name -> value))

  def toQueue(name: String): JmsByteStringMessage = to(Queue(name))

  def toTopic(name: String): JmsByteStringMessage = to(Topic(name))

  def to(destination: Destination): JmsByteStringMessage = copy(destination = Some(destination))

  def withoutDestination: JmsByteStringMessage = copy(destination = None)

  private def copy(bytes: ByteString = bytes,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsByteStringMessage = new JmsByteStringMessage(
    bytes,
    headers,
    properties,
    destination
  )

}

object JmsByteStringMessage {

  /**
   * Create a byte message from a ByteString
   */
  def apply(byteString: ByteString) = new JmsByteStringMessage(byteString)

  /**
   * Java API: Create a byte message from a ByteString
   */
  def create(byteString: ByteString) = apply(byteString)

}

final case class JmsMapMessage(body: Map[String, Any],
                               headers: Set[JmsHeader] = Set.empty,
                               properties: Map[String, Any] = Map.empty,
                               destination: Option[Destination] = None)
    extends JmsMessage {

  /**
   * Add a Jms header e.g. JMSType to [[JmsMapMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsMapMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property to [[JmsMapMessage]]
   */
  def withProperty(name: String, value: Any): JmsMapMessage = copy(properties = properties + (name -> value))

  def toQueue(name: String): JmsMapMessage = to(Queue(name))

  def toTopic(name: String): JmsMapMessage = to(Topic(name))

  def to(destination: Destination): JmsMapMessage = copy(destination = Some(destination))

  def withoutDestination: JmsMapMessage = copy(destination = None)
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
                                properties: Map[String, Any] = Map.empty,
                                destination: Option[Destination] = None)
    extends JmsMessage {

  /**
   * Add a Jms header e.g. JMSType to [[JmsTextMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsTextMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property to [[JmsTextMessage]]
   */
  def withProperty(name: String, value: Any): JmsTextMessage = copy(properties = properties + (name -> value))

  def toQueue(name: String): JmsTextMessage = to(Queue(name))

  def toTopic(name: String): JmsTextMessage = to(Topic(name))

  def to(destination: Destination): JmsTextMessage = copy(destination = Some(destination))

  def withoutDestination: JmsTextMessage = copy(destination = None)
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
                                  properties: Map[String, Any] = Map.empty,
                                  destination: Option[Destination] = None)
    extends JmsMessage {

  /**
   * Add a Jms header e.g. JMSType to [[JmsObjectMessage]]
   */
  def withHeader(jmsHeader: JmsHeader): JmsObjectMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property to [[JmsObjectMessage]]
   */
  def withProperty(name: String, value: Any): JmsObjectMessage = copy(properties = properties + (name -> value))

  def toQueue(name: String): JmsObjectMessage = to(Queue(name))

  def toTopic(name: String): JmsObjectMessage = to(Topic(name))

  def to(destination: Destination): JmsObjectMessage = copy(destination = Some(destination))

  def withoutDestination: JmsObjectMessage = copy(destination = None)
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
