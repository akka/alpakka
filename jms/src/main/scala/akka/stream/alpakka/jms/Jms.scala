/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import java.util
import java.time.Duration
import javax.jms
import javax.jms.ConnectionFactory

import scala.collection.JavaConverters._

sealed trait JmsSettings {
  def connectionFactory: ConnectionFactory
  def destination: Option[Destination]
  def credentials: Option[Credentials]
  def acknowledgeMode: AcknowledgeMode
}

sealed trait Destination
final case class Topic(name: String) extends Destination
final case class Queue(name: String) extends Destination

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
   * Java API: create  [[JmsCorrelationId]]
   */
  def queue(name: String) = JmsReplyTo(Queue(name))

  /**
   * Java API: create  [[JmsCorrelationId]]
   */
  def topic(name: String) = JmsReplyTo(Topic(name))
}

object JmsType {

  /**
   * Java API: create  [[JmsCorrelationId]]
   */
  def create(jmsType: String) = JmsType(jmsType)
}

final case class JmsTextMessage(body: String,
                                headers: Set[JmsHeader] = Set.empty,
                                properties: Map[String, Any] = Map.empty) {

  /**
   * Java API: defines JMSType [[JmsTextMessage]]
   */
  def withHeader(jmsHeader: JmsHeader) = copy(headers = headers + jmsHeader)

  /**
   * Java API: adds JMSProperty [[JmsTextMessage]]
   */
  def withProperty(name: String, value: Any) = copy(properties = properties + (name -> value))

  /**
   * Java API: add [[JmsTextMessage]]
   */
  @deprecated("Unclear method name, use withProperty instead")
  def add(name: String, value: Any) = withProperty(name, value)
}

object JmsTextMessage {

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String) = JmsTextMessage(body = body, headers = Set.empty, properties = Map.empty)

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String, headers: util.Set[JmsHeader]) =
    JmsTextMessage(body = body, headers = headers.asScala.toSet, properties = Map.empty)

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String, properties: util.Map[String, Any]) =
    JmsTextMessage(body = body, headers = Set.empty, properties = properties.asScala.toMap)

  /**
   * Java API: create  [[JmsTextMessage]]
   */
  def create(body: String, headers: util.Set[JmsHeader], properties: util.Map[String, Any]) =
    JmsTextMessage(body = body, headers = headers.asScala.toSet, properties = properties.asScala.toMap)
}

trait AcknowledgeMode {
  val jmsAcknowledgeMode: Int
}

case object AutoAcknowledge extends AcknowledgeMode {
  override val jmsAcknowledgeMode: Int = jms.Session.AUTO_ACKNOWLEDGE
}

case object ClientAcknowledge extends AcknowledgeMode {
  override val jmsAcknowledgeMode: Int = jms.Session.CLIENT_ACKNOWLEDGE
}

case object DupsOkAcknowledge extends AcknowledgeMode {
  override val jmsAcknowledgeMode: Int = jms.Session.DUPS_OK_ACKNOWLEDGE
}

case object SessionTransacted extends AcknowledgeMode {
  override val jmsAcknowledgeMode: Int = jms.Session.SESSION_TRANSACTED
}

object JmsSourceSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSourceSettings(connectionFactory)

}

final case class JmsSourceSettings(connectionFactory: ConnectionFactory,
                                   destination: Option[Destination] = None,
                                   credentials: Option[Credentials] = None,
                                   bufferSize: Int = 100,
                                   selector: Option[String] = None,
                                   acknowledgeMode: AcknowledgeMode = AutoAcknowledge)
    extends JmsSettings {
  def withCredential(credentials: Credentials) = copy(credentials = Some(credentials))
  def withBufferSize(size: Int) = copy(bufferSize = size)
  def withQueue(name: String) = copy(destination = Some(Queue(name)))
  def withTopic(name: String) = copy(destination = Some(Topic(name)))
  def withSelector(selector: String) = copy(selector = Some(selector))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode) = copy(acknowledgeMode = acknowledgeMode)
}

object JmsSinkSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSinkSettings(connectionFactory)

}

final case class JmsSinkSettings(connectionFactory: ConnectionFactory,
                                 destination: Option[Destination] = None,
                                 credentials: Option[Credentials] = None,
                                 timeToLive: Option[Duration] = None,
                                 acknowledgeMode: AcknowledgeMode = AutoAcknowledge)
    extends JmsSettings {
  def withCredential(credentials: Credentials) = copy(credentials = Some(credentials))
  def withQueue(name: String) = copy(destination = Some(Queue(name)))
  def withTopic(name: String) = copy(destination = Some(Topic(name)))
  def withTimeToLive(ttl: Duration) = copy(timeToLive = Some(ttl))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode) = copy(acknowledgeMode = acknowledgeMode)
}

final case class Credentials(username: String, password: String)
