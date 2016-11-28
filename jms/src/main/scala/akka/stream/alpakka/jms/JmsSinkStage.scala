/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import javax.jms.{JMSException, MessageProducer, TextMessage}

import akka.Done
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, StageLogging}
import akka.stream.{Attributes, Inlet, SinkShape}

import scala.concurrent.Future

final class JmsSinkStage(settings: JmsSettings) extends GraphStage[SinkShape[String]] {

  private val in = Inlet[String]("JmsSink.in")

  override def shape: SinkShape[String] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector with StageLogging {

      private var jmsProducer: MessageProducer = _

      override private[jms] def jmsSettings = settings

      private val initProducer = getAsyncCallback[MessageProducer](producer => {
        jmsProducer = producer
        pull(in)
      })

      private val askNext = getAsyncCallback[Done](_ => pull(in))

      override private[jms] def onSessionOpened(): Unit =
        jmsSession.createProducer().foreach { producer =>
          initProducer.invoke(producer)
        }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          Future {
            val textMessage: TextMessage = jmsSession.session.createTextMessage(elem)
            try {
              jmsProducer.send(textMessage)
              askNext.invoke(Done)
            } catch {
              case e: JMSException =>
                fail.invoke(e)
            }
          }
        }
      })

    }

}
