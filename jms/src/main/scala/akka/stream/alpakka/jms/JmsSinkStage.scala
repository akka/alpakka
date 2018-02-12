/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import javax.jms
import javax.jms.{Connection, Message, MessageProducer, Session}

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream._

import scala.concurrent.{Future, Promise}

final class JmsSinkStage(settings: JmsSinkSettings)
    extends GraphStageWithMaterializedValue[SinkShape[JmsMessage], Future[Done]] {

  private val in = Inlet[JmsMessage]("JmsSink.in")

  override def shape: SinkShape[JmsMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes =
    ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val completionPromise = Promise[Done]()
    val logic = new GraphStageLogic(shape) with JmsConnector {

      private var jmsProducer: MessageProducer = _
      private var jmsSession: JmsSession = _

      private[jms] def jmsSettings = settings

      private[jms] def createSession(connection: Connection, createDestination: Session => jms.Destination) = {
        val session =
          connection.createSession(false, settings.acknowledgeMode.getOrElse(AcknowledgeMode.AutoAcknowledge).mode)
        new JmsSession(connection, session, createDestination(session))
      }

      override def preStart(): Unit = {

        jmsSessions = openSessions()
        // TODO: Remove hack to limit publisher to single session.
        jmsSession = jmsSessions.head
        jmsProducer = jmsSession.session.createProducer(jmsSession.destination)
        if (settings.timeToLive.nonEmpty) {
          jmsProducer.setTimeToLive(settings.timeToLive.get.toMillis)
        }
        pull(in)
      }

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish(): Unit = {
            super.onUpstreamFinish()
            completionPromise.trySuccess(Done)
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            super.onUpstreamFailure(ex)
            completionPromise.tryFailure(ex)
          }

          override def onPush(): Unit = {

            val elem: JmsMessage = grab(in)

            val message: Message = createMessage(jmsSession, elem)
            populateMessageProperties(message, elem.properties)

            val (sendHeaders, headersBeforeSend: Set[JmsHeader]) = elem.headers.partition(_.usedDuringSend)
            populateMessageHeader(message, headersBeforeSend)

            val deliveryModeOption = findHeader(sendHeaders) { case x: JmsDeliveryMode => x.deliveryMode }
            val priorityOption = findHeader(sendHeaders) { case x: JmsPriority => x.priority }
            val timeToLiveInMillisOption = findHeader(sendHeaders) { case x: JmsTimeToLive => x.timeInMillis }

            jmsProducer.send(
              message,
              deliveryModeOption.getOrElse(jmsProducer.getDeliveryMode),
              priorityOption.getOrElse(jmsProducer.getPriority),
              timeToLiveInMillisOption.getOrElse(jmsProducer.getTimeToLive)
            )

            pull(in)
          }
        }
      )

      private def findHeader[T](headersDuringSend: Set[JmsHeader])(f: PartialFunction[JmsHeader, T]): Option[T] =
        headersDuringSend.collectFirst(f)

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

      override def postStop(): Unit = {
        if (!completionPromise.isCompleted) completionPromise.tryFailure(new AbruptStageTerminationException(this))
        jmsSessions.foreach(_.closeSession())
        jmsConnection.foreach(_.close)
      }
    }

    (logic, completionPromise.future)

  }

}
