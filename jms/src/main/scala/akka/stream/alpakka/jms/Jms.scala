/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import java.time.Duration
import javax.jms
import javax.jms.ConnectionFactory

sealed trait JmsSettings {
  def connectionFactory: ConnectionFactory
  def destination: Option[Destination]
  def credentials: Option[Credentials]
  def acknowledgeMode: AcknowledgeMode
}

sealed trait Destination
final case class Topic(name: String) extends Destination
final case class Queue(name: String) extends Destination

final class AcknowledgeMode(val mode: Int)

object AcknowledgeMode {
  val AutoAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.AUTO_ACKNOWLEDGE)
  val ClientAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.CLIENT_ACKNOWLEDGE)
  val DupsOkAcknowledge: AcknowledgeMode = new AcknowledgeMode(jms.Session.DUPS_OK_ACKNOWLEDGE)
  val SessionTransacted: AcknowledgeMode = new AcknowledgeMode(jms.Session.SESSION_TRANSACTED)
}

object JmsSourceSettings {

  def create(connectionFactory: ConnectionFactory) = JmsSourceSettings(connectionFactory)

}

final case class JmsSourceSettings(connectionFactory: ConnectionFactory,
                                   destination: Option[Destination] = None,
                                   credentials: Option[Credentials] = None,
                                   bufferSize: Int = 100,
                                   selector: Option[String] = None,
                                   acknowledgeMode: AcknowledgeMode = AcknowledgeMode.AutoAcknowledge)
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
                                 acknowledgeMode: AcknowledgeMode = AcknowledgeMode.AutoAcknowledge)
    extends JmsSettings {
  def withCredential(credentials: Credentials) = copy(credentials = Some(credentials))
  def withQueue(name: String) = copy(destination = Some(Queue(name)))
  def withTopic(name: String) = copy(destination = Some(Topic(name)))
  def withTimeToLive(ttl: Duration) = copy(timeToLive = Some(ttl))
  def withAcknowledgeMode(acknowledgeMode: AcknowledgeMode) = copy(acknowledgeMode = acknowledgeMode)
}

final case class Credentials(username: String, password: String)
