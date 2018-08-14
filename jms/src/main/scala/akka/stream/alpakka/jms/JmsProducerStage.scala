/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.alpakka.jms.JmsProducerStage._
import akka.stream.impl.{Buffer, ReactiveStreamsCompliance}
import akka.stream.stage._
import akka.util.OptionVal
import javax.jms
import javax.jms.{Connection, Session}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.{NoStackTrace, NonFatal}

private[jms] final class JmsProducerStage[A <: JmsMessage](settings: JmsProducerSettings)
    extends GraphStage[FlowShape[A, A]] {

  private val in = Inlet[A]("JmsProducer.in")
  private val out = Outlet[A]("JmsProducer.out")

  override def shape: FlowShape[A, A] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with JmsConnector {

      private lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private val jmsProducers: Buffer[JmsMessageProducer] = Buffer(settings.sessionCount, settings.sessionCount)

      private val buffer: Buffer[Holder[A]] = Buffer(settings.sessionCount, settings.sessionCount)

      private[jms] def jmsSettings = settings

      private[jms] def createSession(connection: Connection, createDestination: Session => jms.Destination) = {
        val session =
          connection.createSession(false, settings.acknowledgeMode.getOrElse(AcknowledgeMode.AutoAcknowledge).mode)
        new JmsSession(connection, session, createDestination(session))
      }

      override def preStart(): Unit = {
        jmsSessions = openSessions()
        jmsSessions.foreach(jmsSession => jmsProducers.enqueue(JmsMessageProducer(jmsSession, settings)))
        ec = executionContext(inheritedAttributes)
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pushNextIfPossible()
      })

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish(): Unit = if (buffer.isEmpty) completeStage()

          override def onPush(): Unit = {
            val elem: A = grab(in)
            val jmsProducer = jmsProducers.dequeue() // fetch jms producer from the pool.
            val holder = new Holder(NotYetThere, futureCB, jmsProducer)
            buffer.enqueue(holder)
            Future {
              jmsProducer.send(elem)
              elem
            }.onComplete(holder)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
            pullIfNeeded()
          }
        }
      )

      override def postStop(): Unit = {
        jmsSessions.foreach(_.closeSession())
        jmsConnection.foreach(_.close)
      }

      private val futureCB = getAsyncCallback[Holder[A]](
        holder =>
          holder.elem match {
            case Success(_) => pushNextIfPossible()
            case Failure(ex) => handleFailure(ex, holder)
        }
      )

      private def pullIfNeeded(): Unit =
        if (jmsProducers.nonEmpty && !hasBeenPulled(in)) tryPull(in) // only pull if a producer is available in the pool.

      // heavily inspired by akka.stream.impl.fusing.MapAsync
      private def pushNextIfPossible(): Unit =
        if (buffer.isEmpty) {
          if (isClosed(in)) completeStage() else pullIfNeeded()
        } else if (buffer.peek().elem eq NotYetThere) {
          pullIfNeeded() // ahead of line blocking to keep order
        } else if (isAvailable(out)) {
          val holder = buffer.dequeue()
          holder.elem match {
            case Success(elem) =>
              push(out, elem)
              jmsProducers.enqueue(holder.jmsProducer) // put back jms producer to the pool.
              pullIfNeeded()

            case Failure(NonFatal(ex)) => handleFailure(ex, holder)
          }
        }

      private def handleFailure(ex: Throwable, holder: Holder[A]): Unit =
        holder.supervisionDirectiveFor(decider, ex) match {
          case Supervision.Stop => failStage(ex)
          case _ => pushNextIfPossible()
        }
    }
}

private[jms] object JmsProducerStage {

  val NotYetThere = Failure(new Exception with NoStackTrace)

  // heavily inspired by akka.stream.impl.fusing.MapAsync
  class Holder[A <: JmsMessage](var elem: Try[A], val cb: AsyncCallback[Holder[A]], val jmsProducer: JmsMessageProducer)
      extends (Try[A] => Unit) {

    private var cachedSupervisionDirective: OptionVal[Supervision.Directive] = OptionVal.None

    def supervisionDirectiveFor(decider: Supervision.Decider, ex: Throwable): Supervision.Directive =
      cachedSupervisionDirective match {
        case OptionVal.Some(d) ⇒ d
        case OptionVal.None ⇒
          val d = decider(ex)
          cachedSupervisionDirective = OptionVal.Some(d)
          d
      }

    def setElem(t: Try[A]): Unit =
      elem = t match {
        case Success(null) ⇒ Failure[A](ReactiveStreamsCompliance.elementMustNotBeNullException)
        case other ⇒ other
      }

    override def apply(t: Try[A]): Unit = {
      setElem(t)
      cb.invoke(this)
    }
  }
}
