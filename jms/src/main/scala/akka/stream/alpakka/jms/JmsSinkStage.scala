/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import javax.jms.{MessageProducer, TextMessage}

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}

final class JmsSinkStage(settings: JmsSettings) extends GraphStage[SinkShape[JmsMessage]] {

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
        pull(in)
      }

      setHandler(in,
        new InHandler {
        override def onPush(): Unit = {
          val elem: JmsMessage = grab(in)
          val textMessage: TextMessage = jmsSession.session.createTextMessage(elem.body)
          elem.properties.map {
            case (key, v) =>
              v match {
                case v: String => textMessage.setStringProperty(key, v)
                case v: Int => textMessage.setIntProperty(key, v)
                case v: Boolean => textMessage.setBooleanProperty(key, v)
                case v: Byte => textMessage.setByteProperty(key, v)
                // TODO: map the rest!
              }
          }
          jmsProducer.send(textMessage)
          pull(in)
        }
      })

      override def postStop(): Unit =
        Option(jmsSession).foreach(_.closeSession())
    }

}
