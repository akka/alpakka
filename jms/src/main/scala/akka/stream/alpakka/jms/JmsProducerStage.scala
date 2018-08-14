/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.stream._
import akka.stream.stage._
import javax.jms
import javax.jms.{Connection, Session}

private[jms] final class JmsProducerStage[A <: JmsMessage](settings: JmsProducerSettings)
    extends GraphStage[FlowShape[A, A]] {

  private val in = Inlet[A]("JmsProducer.in")
  private val out = Outlet[A]("JmsProducer.out")

  override def shape: FlowShape[A, A] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector {

      private var jmsProducer: JmsMessageProducer = _
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
        jmsProducer = JmsMessageProducer(jmsSession, settings)
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          tryPull(in)
      })

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem: A = grab(in)
            jmsProducer.send(elem)
            push(out, elem)
          }
        }
      )

      override def postStop(): Unit = {
        jmsSessions.foreach(_.closeSession())
        jmsConnection.foreach(_.close)
      }
    }

}
