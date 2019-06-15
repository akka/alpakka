/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.util

import akka.NotUsed
import akka.util.ByteString

import scala.collection.JavaConverters._

/**
 * Base interface for messages handled by JmsProducers. Sub-classes support pass-through or use [[akka.NotUsed]] as type for pass-through.
 *
 * @tparam PassThrough the type of data passed through the `flexiFlow`
 */
sealed trait JmsEnvelope[+PassThrough] {
  def headers: Set[JmsHeader]
  def properties: Map[String, Any]
  def destination: Option[Destination]
  def passThrough: PassThrough
}

/**
 * A stream element that does not produce a JMS message, but merely passes data through a `flexiFlow`.
 *
 * @param passThrough the data to pass through
 * @tparam PassThrough the type of data passed through the `flexiFlow`
 */
final class JmsPassThrough[+PassThrough](val passThrough: PassThrough) extends JmsEnvelope[PassThrough] {
  val properties: Map[String, Any] = Map.empty
  val headers: Set[JmsHeader] = Set.empty
  val destination: Option[Destination] = None
}

/**
 * A stream element that does not produce a JMS message, but merely passes data through a `flexiFlow`.
 */
object JmsPassThrough {
  def apply[PassThrough](passThrough: PassThrough): JmsEnvelope[PassThrough] =
    new JmsPassThrough[PassThrough](passThrough)
  def create[PassThrough](passThrough: PassThrough): JmsEnvelope[PassThrough] =
    new JmsPassThrough[PassThrough](passThrough)
}

/**
 * Marker trait for stream elements that do not contain pass-through data.
 */
sealed trait JmsMessage extends JmsEnvelope[NotUsed]

/**
 * Produces byte arrays to JMS, supports pass-through data.
 *
 * @tparam PassThrough the type of data passed through the `flexiFlow`
 */
sealed class JmsByteMessagePassThrough[+PassThrough] protected[jms] (val bytes: Array[Byte],
                                                                     val headers: Set[JmsHeader] = Set.empty,
                                                                     val properties: Map[String, Any] = Map.empty,
                                                                     val destination: Option[Destination] = None,
                                                                     val passThrough: PassThrough)
    extends JmsEnvelope[PassThrough] {

  /**
   * Add a Jms header e.g. JMSType
   */
  def withHeader(jmsHeader: JmsHeader): JmsByteMessagePassThrough[PassThrough] = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  def withProperty(name: String, value: Any): JmsByteMessagePassThrough[PassThrough] =
    copy(properties = properties + (name -> value))

  def withProperties(props: Map[String, Any]): JmsByteMessagePassThrough[PassThrough] =
    copy(properties = properties ++ props)

  def toQueue(name: String): JmsByteMessagePassThrough[PassThrough] = to(Queue(name))

  def toTopic(name: String): JmsByteMessagePassThrough[PassThrough] = to(Topic(name))

  def to(destination: Destination): JmsByteMessagePassThrough[PassThrough] = copy(destination = Some(destination))

  def withoutDestination: JmsByteMessagePassThrough[PassThrough] = copy(destination = None)

  def withPassThrough[PassThrough2](passThrough: PassThrough2): JmsByteMessagePassThrough[PassThrough2] =
    new JmsByteMessagePassThrough(
      bytes,
      headers,
      properties,
      destination,
      passThrough
    )

  private def copy(bytes: Array[Byte] = bytes,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsByteMessagePassThrough[PassThrough] =
    new JmsByteMessagePassThrough[PassThrough](
      bytes,
      headers,
      properties,
      destination,
      passThrough
    )
}

/**
 * Produces byte arrays to JMS.
 */
final class JmsByteMessage private (bytes: Array[Byte],
                                    headers: Set[JmsHeader] = Set.empty,
                                    properties: Map[String, Any] = Map.empty,
                                    destination: Option[Destination] = None)
    extends JmsByteMessagePassThrough[NotUsed](bytes, headers, properties, destination, NotUsed)
    with JmsMessage {

  /**
   * Add a Jms header e.g. JMSType
   */
  override def withHeader(jmsHeader: JmsHeader): JmsByteMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  override def withProperty(name: String, value: Any): JmsByteMessage =
    copy(properties = properties + (name -> value))

  override def toQueue(name: String): JmsByteMessage = to(Queue(name))

  override def toTopic(name: String): JmsByteMessage = to(Topic(name))

  override def to(destination: Destination): JmsByteMessage = copy(destination = Some(destination))

  override def withoutDestination: JmsByteMessage = copy(destination = None)

  private def copy(bytes: Array[Byte] = bytes,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsByteMessage =
    new JmsByteMessage(
      bytes,
      headers,
      properties,
      destination
    )

}

/**
 * Produces byte arrays to JMS.
 */
object JmsByteMessage {

  /**
   * create a byte message with pass-through
   */
  def apply[PassThrough](bytes: Array[Byte], passThrough: PassThrough): JmsByteMessagePassThrough[PassThrough] =
    new JmsByteMessagePassThrough[PassThrough](bytes = bytes, passThrough = passThrough)

  /**
   * create a byte message
   */
  def apply(bytes: Array[Byte]) = new JmsByteMessage(bytes = bytes)

  /**
   * Java API: create a byte message with pass-through
   */
  def create[PassThrough](bytes: Array[Byte], passThrough: PassThrough): JmsByteMessagePassThrough[PassThrough] =
    new JmsByteMessagePassThrough[PassThrough](bytes = bytes, passThrough = passThrough)

  /**
   * Java API: create [[JmsByteMessage]]
   */
  def create(bytes: Array[Byte]) = new JmsByteMessage(bytes = bytes)

}

/**
 * Produces byte array messages to JMS from the incoming `ByteString`, supports pass-through data.
 *
 * @tparam PassThrough the type of data passed through the `flexiFlow`
 */
sealed class JmsByteStringMessagePassThrough[+PassThrough] protected[jms] (val bytes: ByteString,
                                                                           val headers: Set[JmsHeader] = Set.empty,
                                                                           val properties: Map[String, Any] = Map.empty,
                                                                           val destination: Option[Destination] = None,
                                                                           val passThrough: PassThrough)
    extends JmsEnvelope[PassThrough] {

  /**
   * Add a Jms header e.g. JMSType
   */
  def withHeader(jmsHeader: JmsHeader): JmsByteStringMessagePassThrough[PassThrough] =
    copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  def withProperty(name: String, value: Any): JmsByteStringMessagePassThrough[PassThrough] =
    copy(properties = properties + (name -> value))

  def withProperties(props: Map[String, Any]): JmsByteStringMessagePassThrough[PassThrough] =
    copy(properties = properties ++ props)

  def toQueue(name: String): JmsByteStringMessagePassThrough[PassThrough] = to(Queue(name))

  def toTopic(name: String): JmsByteStringMessagePassThrough[PassThrough] = to(Topic(name))

  def to(destination: Destination): JmsByteStringMessagePassThrough[PassThrough] = copy(destination = Some(destination))

  def withoutDestination: JmsByteStringMessagePassThrough[PassThrough] = copy(destination = None)

  def withPassThrough[PassThrough2](passThrough: PassThrough2): JmsByteStringMessagePassThrough[PassThrough2] =
    new JmsByteStringMessagePassThrough(
      bytes,
      headers,
      properties,
      destination,
      passThrough
    )

  private def copy(bytes: ByteString = bytes,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsByteStringMessagePassThrough[PassThrough] =
    new JmsByteStringMessagePassThrough(
      bytes,
      headers,
      properties,
      destination,
      passThrough
    )

}

/**
 * Produces byte array messages to JMS from the incoming `ByteString`.
 */
final class JmsByteStringMessage private (bytes: ByteString,
                                          headers: Set[JmsHeader] = Set.empty,
                                          properties: Map[String, Any] = Map.empty,
                                          destination: Option[Destination] = None)
    extends JmsByteStringMessagePassThrough[NotUsed](bytes, headers, properties, destination, NotUsed)
    with JmsMessage {

  /**
   * Add a Jms header e.g. JMSType
   */
  override def withHeader(jmsHeader: JmsHeader): JmsByteStringMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  override def withProperty(name: String, value: Any): JmsByteStringMessage =
    copy(properties = properties + (name -> value))

  override def toQueue(name: String): JmsByteStringMessage = to(Queue(name))

  override def toTopic(name: String): JmsByteStringMessage = to(Topic(name))

  override def to(destination: Destination): JmsByteStringMessage = copy(destination = Some(destination))

  override def withoutDestination: JmsByteStringMessage = copy(destination = None)

  private def copy(bytes: ByteString = bytes,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsByteStringMessage =
    new JmsByteStringMessage(
      bytes,
      headers,
      properties,
      destination
    )

}

/**
 * Produces byte array messages to JMS from the incoming `ByteString`.
 */
object JmsByteStringMessage {

  /**
   * Create a byte message from a ByteString with a pass-through attached
   */
  def apply[PassThrough](byteString: ByteString,
                         passThrough: PassThrough): JmsByteStringMessagePassThrough[PassThrough] =
    new JmsByteStringMessagePassThrough[PassThrough](byteString, passThrough = passThrough)

  /**
   * Create a byte message from a ByteString
   */
  def apply(byteString: ByteString) = new JmsByteStringMessage(byteString)

  /**
   * Java API: Create a byte message from a ByteString with a pass-through attached
   */
  def create[PassThrough](byteString: ByteString,
                          passThrough: PassThrough): JmsByteStringMessagePassThrough[PassThrough] =
    new JmsByteStringMessagePassThrough[PassThrough](byteString, passThrough = passThrough)

  /**
   * Java API: Create a byte message from a ByteString
   */
  def create(byteString: ByteString) = apply(byteString)

}

/**
 * Produces map messages to JMS, supports pass-through data.
 *
 * @tparam PassThrough the type of data passed through the `flexiFlow`
 */
sealed class JmsMapMessagePassThrough[+PassThrough] protected[jms] (val body: Map[String, Any],
                                                                    val headers: Set[JmsHeader] = Set.empty,
                                                                    val properties: Map[String, Any] = Map.empty,
                                                                    val destination: Option[Destination] = None,
                                                                    val passThrough: PassThrough)
    extends JmsEnvelope[PassThrough] {

  /**
   * Add a Jms header e.g. JMSType
   */
  def withHeader(jmsHeader: JmsHeader): JmsMapMessagePassThrough[PassThrough] = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  def withProperty(name: String, value: Any): JmsMapMessagePassThrough[PassThrough] =
    copy(properties = properties + (name -> value))

  def withProperties(props: Map[String, Any]): JmsMapMessagePassThrough[PassThrough] =
    copy(properties = properties ++ props)

  def toQueue(name: String): JmsMapMessagePassThrough[PassThrough] = to(Queue(name))

  def toTopic(name: String): JmsMapMessagePassThrough[PassThrough] = to(Topic(name))

  def to(destination: Destination): JmsMapMessagePassThrough[PassThrough] = copy(destination = Some(destination))

  def withoutDestination: JmsMapMessagePassThrough[PassThrough] = copy(destination = None)

  def withPassThrough[PassThrough2](passThrough: PassThrough2): JmsMapMessagePassThrough[PassThrough2] =
    new JmsMapMessagePassThrough(
      body,
      headers,
      properties,
      destination,
      passThrough
    )

  private def copy(body: Map[String, Any] = body,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsMapMessagePassThrough[PassThrough] =
    new JmsMapMessagePassThrough(
      body,
      headers,
      properties,
      destination,
      passThrough
    )

}

/**
 * Produces map messages to JMS.
 */
final class JmsMapMessage(body: Map[String, Any],
                          headers: Set[JmsHeader] = Set.empty,
                          properties: Map[String, Any] = Map.empty,
                          destination: Option[Destination] = None)
    extends JmsMapMessagePassThrough[NotUsed](body, headers, properties, destination, NotUsed)
    with JmsMessage {

  /**
   * Add a Jms header e.g. JMSType
   */
  override def withHeader(jmsHeader: JmsHeader): JmsMapMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  override def withProperty(name: String, value: Any): JmsMapMessage =
    copy(properties = properties + (name -> value))

  override def toQueue(name: String): JmsMapMessage = to(Queue(name))

  override def toTopic(name: String): JmsMapMessage = to(Topic(name))

  override def to(destination: Destination): JmsMapMessage = copy(destination = Some(destination))

  override def withoutDestination: JmsMapMessage = copy(destination = None)

  private def copy(body: Map[String, Any] = body,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsMapMessage =
    new JmsMapMessage(
      body,
      headers,
      properties,
      destination
    )

}

/**
 * Produces map messages to JMS.
 */
object JmsMapMessage {

  /**
   * create a map message with a pass-through attached
   */
  def apply[PassThrough](map: Map[String, Any], passThrough: PassThrough): JmsMapMessagePassThrough[PassThrough] =
    new JmsMapMessagePassThrough[PassThrough](body = map, passThrough = passThrough)

  /**
   * create a map message
   */
  def apply(map: Map[String, Any]) = new JmsMapMessage(body = map)

  /**
   * Java API: create a map message with a pass-through attached
   */
  def create[PassThrough](map: java.util.Map[String, Any],
                          passThrough: PassThrough): JmsMapMessagePassThrough[PassThrough] =
    new JmsMapMessagePassThrough[PassThrough](body = map.asScala.toMap, passThrough = passThrough)

  /**
   * Java API: create map message
   */
  def create(map: util.Map[String, Any]) = new JmsMapMessage(body = map.asScala.toMap)

}

/**
 * Produces text messages to JMS, supports pass-through data.
 *
 * @tparam PassThrough the type of data passed through the `flexiFlow`
 */
sealed class JmsTextMessagePassThrough[+PassThrough] protected[jms] (val body: String,
                                                                     val headers: Set[JmsHeader] = Set.empty,
                                                                     val properties: Map[String, Any] = Map.empty,
                                                                     val destination: Option[Destination] = None,
                                                                     val passThrough: PassThrough)
    extends JmsEnvelope[PassThrough] {

  /**
   * Add a Jms header e.g. JMSType
   */
  def withHeader(jmsHeader: JmsHeader): JmsTextMessagePassThrough[PassThrough] = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  def withProperty(name: String, value: Any): JmsTextMessagePassThrough[PassThrough] =
    copy(properties = properties + (name -> value))

  def withProperties(props: Map[String, Any]): JmsTextMessagePassThrough[PassThrough] =
    copy(properties = properties ++ props)

  def toQueue(name: String): JmsTextMessagePassThrough[PassThrough] = to(Queue(name))

  def toTopic(name: String): JmsTextMessagePassThrough[PassThrough] = to(Topic(name))

  def to(destination: Destination): JmsTextMessagePassThrough[PassThrough] = copy(destination = Some(destination))

  def withoutDestination: JmsTextMessagePassThrough[PassThrough] = copy(destination = None)

  def withPassThrough[PassThrough2](passThrough: PassThrough2): JmsTextMessagePassThrough[PassThrough2] =
    new JmsTextMessagePassThrough(
      body,
      headers,
      properties,
      destination,
      passThrough
    )

  private def copy(body: String = body,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsTextMessagePassThrough[PassThrough] =
    new JmsTextMessagePassThrough[PassThrough](
      body,
      headers,
      properties,
      destination,
      passThrough
    )

}

/**
 * Produces text messages to JMS.
 */
final class JmsTextMessage private (body: String,
                                    headers: Set[JmsHeader] = Set.empty,
                                    properties: Map[String, Any] = Map.empty,
                                    destination: Option[Destination] = None)
    extends JmsTextMessagePassThrough[NotUsed](body, headers, properties, destination, NotUsed)
    with JmsMessage {

  /**
   * Add a Jms header e.g. JMSType
   */
  override def withHeader(jmsHeader: JmsHeader): JmsTextMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  override def withProperty(name: String, value: Any): JmsTextMessage = copy(properties = properties + (name -> value))

  override def toQueue(name: String): JmsTextMessage = to(Queue(name))

  override def toTopic(name: String): JmsTextMessage = to(Topic(name))

  override def to(destination: Destination): JmsTextMessage = copy(destination = Some(destination))

  override def withoutDestination: JmsTextMessage = copy(destination = None)

  private def copy(body: String = body,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsTextMessage = new JmsTextMessage(
    body,
    headers,
    properties,
    destination
  )

}

/**
 * Produces text messages to JMS.
 */
object JmsTextMessage {

  /**
   * Create a text message with a pass-through attached
   */
  def apply[PassThrough](body: String, passThrough: PassThrough): JmsTextMessagePassThrough[PassThrough] =
    new JmsTextMessagePassThrough(body = body, passThrough = passThrough)

  /**
   * Create a text message
   */
  def apply(body: String): JmsTextMessage = new JmsTextMessage(body = body)

  /**
   * Java API: Create a text message with a pass-through attached
   */
  def create[PassThrough](body: String, passThrough: PassThrough): JmsTextMessagePassThrough[PassThrough] =
    new JmsTextMessagePassThrough(body = body, passThrough = passThrough)

  /**
   * Java API: create a text message
   */
  def create(body: String): JmsTextMessage = new JmsTextMessage(body = body)

}

/**
 * Produces object messages to JMS, supports pass-through data.
 *
 * @tparam PassThrough the type of data passed through the `flexiFlow`
 */
sealed class JmsObjectMessagePassThrough[+PassThrough] protected[jms] (val serializable: java.io.Serializable,
                                                                       val headers: Set[JmsHeader] = Set.empty,
                                                                       val properties: Map[String, Any] = Map.empty,
                                                                       val destination: Option[Destination] = None,
                                                                       val passThrough: PassThrough)
    extends JmsEnvelope[PassThrough] {

  /**
   * Add a Jms header e.g. JMSType
   */
  def withHeader(jmsHeader: JmsHeader): JmsObjectMessagePassThrough[PassThrough] = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  def withProperty(name: String, value: Any): JmsObjectMessagePassThrough[PassThrough] =
    copy(properties = properties + (name -> value))

  def withProperties(props: Map[String, Any]): JmsObjectMessagePassThrough[PassThrough] =
    copy(properties = properties ++ props)

  def toQueue(name: String): JmsObjectMessagePassThrough[PassThrough] = to(Queue(name))

  def toTopic(name: String): JmsObjectMessagePassThrough[PassThrough] = to(Topic(name))

  def to(destination: Destination): JmsObjectMessagePassThrough[PassThrough] = copy(destination = Some(destination))

  def withoutDestination: JmsObjectMessagePassThrough[PassThrough] = copy(destination = None)

  def withPassThrough[PassThrough2](passThrough: PassThrough2): JmsObjectMessagePassThrough[PassThrough2] =
    new JmsObjectMessagePassThrough(
      serializable,
      headers,
      properties,
      destination,
      passThrough
    )

  private def copy(serializable: java.io.Serializable = serializable,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsObjectMessagePassThrough[PassThrough] =
    new JmsObjectMessagePassThrough(
      serializable,
      headers,
      properties,
      destination,
      passThrough
    )

}

/**
 * Produces object messages to JMS.
 */
final class JmsObjectMessage private (serializable: java.io.Serializable,
                                      headers: Set[JmsHeader] = Set.empty,
                                      properties: Map[String, Any] = Map.empty,
                                      destination: Option[Destination] = None)
    extends JmsObjectMessagePassThrough[NotUsed](serializable, headers, properties, destination, NotUsed)
    with JmsMessage {

  /**
   * Add a Jms header e.g. JMSType
   */
  override def withHeader(jmsHeader: JmsHeader): JmsObjectMessage = copy(headers = headers + jmsHeader)

  /**
   * Add a property
   */
  override def withProperty(name: String, value: Any): JmsObjectMessage =
    copy(properties = properties + (name -> value))

  override def toQueue(name: String): JmsObjectMessage = to(Queue(name))

  override def toTopic(name: String): JmsObjectMessage = to(Topic(name))

  override def to(destination: Destination): JmsObjectMessage = copy(destination = Some(destination))

  override def withoutDestination: JmsObjectMessage = copy(destination = None)

  private def copy(serializable: java.io.Serializable = serializable,
                   headers: Set[JmsHeader] = headers,
                   properties: Map[String, Any] = properties,
                   destination: Option[Destination] = destination): JmsObjectMessage =
    new JmsObjectMessage(
      serializable,
      headers,
      properties,
      destination
    )

}

/**
 * Produces object messages to JMS.
 */
object JmsObjectMessage {

  /**
   * create an object message with a pass-through attached
   */
  def apply[PassThrough](serializable: java.io.Serializable,
                         passThrough: PassThrough): JmsObjectMessagePassThrough[PassThrough] =
    new JmsObjectMessagePassThrough[PassThrough](serializable, passThrough = passThrough)

  /**
   * create an object message
   */
  def apply(serializable: java.io.Serializable) = new JmsObjectMessage(serializable)

  /**
   * Java API: create an object message with a pass-through attached
   */
  def create[PassThrough](serializable: java.io.Serializable,
                          passThrough: PassThrough): JmsObjectMessagePassThrough[PassThrough] =
    new JmsObjectMessagePassThrough[PassThrough](serializable, passThrough = passThrough)

  /**
   * Java API: create an object message
   */
  def create(serializable: Serializable) = new JmsObjectMessage(serializable)

}
