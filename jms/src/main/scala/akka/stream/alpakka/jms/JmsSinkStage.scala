/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import javax.jms.{MessageProducer, TextMessage}

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}

final class JmsSinkStage(settings: JmsSinkSettings) extends GraphStage[SinkShape[JmsTextMessage]] {

  private val in = Inlet[JmsTextMessage]("JmsSink.in")

  override def shape: SinkShape[JmsTextMessage] = SinkShape.of(in)

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
            val elem: JmsTextMessage = grab(in)
            val textMessage: TextMessage = jmsSession.session.createTextMessage(elem.body)
            elem.properties.foreach {
              case (key, v) =>
                v match {
                  case v: String => textMessage.setStringProperty(key, v)
                  case v: Int => textMessage.setIntProperty(key, v)
                  case v: Boolean => textMessage.setBooleanProperty(key, v)
                  case v: Byte => textMessage.setByteProperty(key, v)
                  case v: Short => textMessage.setShortProperty(key, v)
                  case v: Long => textMessage.setLongProperty(key, v)
                  case v: Double => textMessage.setDoubleProperty(key, v)
                }
            }
            jmsProducer.send(textMessage)
            pull(in)
          }
        }
      )

      override def postStop(): Unit =
        Option(jmsSession).foreach(_.closeSession())
    }

}
