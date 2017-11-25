/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms.{Message, MessageProducer}

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}

final class JmsSinkStage(settings: JmsSinkSettings) extends GraphStage[SinkShape[JmsMessage]] {

  private val in = Inlet[JmsMessage]("JmsSink.in")

  override def shape: SinkShape[JmsMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes =
    ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector {

      private var jmsProducer: MessageProducer = _

      override private[jms] def jmsSettings = settings

      override def preStart(): Unit = {
        jmsSession = openSession()
        jmsProducer = jmsSession.session.createProducer(jmsSession.destination)
        if (settings.timeToLive.nonEmpty) {
          jmsProducer.setTimeToLive(settings.timeToLive.get.toMillis)
        }
        pull(in)
      }

      setHandler(
        in,
        new InHandler {

          override def onPush(): Unit = {

            val elem: JmsMessage = grab(in)
            val message: Message = createMessage(jmsSession, elem)
            populateMessageProperties(message, elem.properties)
            populateMessageHeader(message, elem.headers)

            jmsProducer.send(message)
            pull(in)
          }
        }
      )

      private def createMessage(jmsSession: JmsSession, element: JmsMessage): Message =
        element match {

          case textMessage: JmsTextMessage => jmsSession.session.createTextMessage(textMessage.body)

          case byteMessage: JmsByteMessage =>
            val newMessage = jmsSession.session.createBytesMessage()
            newMessage.writeBytes(byteMessage.bytes)
            newMessage

          case mapMessage: JmsMapMessage =>
            val newMessage = jmsSession.session.createMapMessage()
            populateMapMessage(newMessage, mapMessage.body)
            newMessage

          case objectMessage: JmsObjectMessage => jmsSession.session.createObjectMessage(objectMessage.serializable)

        }

      private def populateMapMessage(message: javax.jms.MapMessage, map: Map[String, Any]): Unit =
        map.foreach {
          case (key, v) =>
            v match {
              case v: String => message.setString(key, v)
              case v: Int => message.setInt(key, v)
              case v: Boolean => message.setBoolean(key, v)
              case v: Byte => message.setByte(key, v)
              case v: Short => message.setShort(key, v)
              case v: Long => message.setLong(key, v)
              case v: Double => message.setDouble(key, v)
              case v: Array[Byte] => message.setBytes(key, v)
            }
        }

      private def populateMessageProperties(message: javax.jms.Message, properties: Map[String, Any]): Unit =
        properties.foreach {
          case (key, v) =>
            v match {
              case v: String => message.setStringProperty(key, v)
              case v: Int => message.setIntProperty(key, v)
              case v: Boolean => message.setBooleanProperty(key, v)
              case v: Byte => message.setByteProperty(key, v)
              case v: Short => message.setShortProperty(key, v)
              case v: Long => message.setLongProperty(key, v)
              case v: Double => message.setDoubleProperty(key, v)
            }
        }

      private def populateMessageHeader(message: javax.jms.Message, headers: Set[JmsHeader]): Unit = {
        def createDestination(destination: Destination): _root_.javax.jms.Destination =
          destination match {
            case Queue(name) => jmsSession.session.createQueue(name)
            case Topic(name) => jmsSession.session.createTopic(name)
          }

        headers.foreach {
          case JmsType(jmsType) => message.setJMSType(jmsType)
          case JmsReplyTo(destination) => message.setJMSReplyTo(createDestination(destination))
          case JmsCorrelationId(jmsCorrelationId) => message.setJMSCorrelationID(jmsCorrelationId)
        }
      }

      override def postStop(): Unit = Option(jmsSession).foreach(_.closeSession())
    }

}
