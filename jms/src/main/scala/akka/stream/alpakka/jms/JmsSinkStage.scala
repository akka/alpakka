/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms

import javax.jms.{ MessageProducer, TextMessage }

import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, StageLogging }
import akka.stream.{ Attributes, Inlet, SinkShape }

class JmsSinkStage(settings: JmsSettings) extends GraphStage[SinkShape[String]] {

  val in = Inlet[String]("JmsSink.in")

  override def shape: SinkShape[String] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector with StageLogging {

      var jmsProducer: Option[MessageProducer] = None

      override def jmsSettings = settings

      override def preStart(): Unit = {
        super.preStart()
        try {
          jmsProducer = for {
            session <- jmsSession
            queue <- jmsDestination
          } yield {
            val producer: MessageProducer = session.createProducer(queue)
            pull(in)
            producer
          }
        } catch {
          case e: Exception =>
            log.error(e, "Error creating producer")
            failStage(e)
        }

      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          for {
            session <- jmsSession
            producer <- jmsProducer
          } yield {
            val textMessage: TextMessage = session.createTextMessage(elem)
            producer.send(textMessage)
            pull(in)
          }
        }
      })

    }

}
