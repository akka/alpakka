/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import javax.jms.{MessageProducer, TextMessage}

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}

final class JmsSinkStage(settings: JmsSettings) extends GraphStage[SinkShape[String]] {

  private val in = Inlet[String]("JmsSink.in")

  override def shape: SinkShape[String] = SinkShape.of(in)

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

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            val textMessage: TextMessage = jmsSession.session.createTextMessage(elem)
            jmsProducer.send(textMessage)
            pull(in)
          }
        }
      )

      override def postStop(): Unit =
        Option(jmsSession).foreach(_.closeSession())
    }

}
