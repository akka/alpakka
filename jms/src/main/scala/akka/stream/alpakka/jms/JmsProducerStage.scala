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

      /*
       * NOTE: the following code is heavily inspired by akka.stream.impl.fusing.MapAsync
       *
       * To get a condensed view of what the buffers and handler behavior is about, have a look there too.
       */

      private lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      // available producers for sending messages. Initially full, but might contain less elements if
      // messages are currently in-flight.
      private val jmsProducers: Buffer[JmsMessageProducer] = Buffer(settings.sessionCount, settings.sessionCount)

      // in-flight messages with the producers that were used to send them.
      private val inFlightMessagesWithProducer: Buffer[Holder[A]] = Buffer(settings.sessionCount, settings.sessionCount)

      protected def jmsSettings: JmsProducerSettings = settings

      protected def createSession(connection: Connection, createDestination: Session => jms.Destination): JmsSession = {
        val session =
          connection.createSession(false, settings.acknowledgeMode.getOrElse(AcknowledgeMode.AutoAcknowledge).mode)
        new JmsSession(connection, session, createDestination(session), settings.destination.get)
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

          override def onUpstreamFinish(): Unit = if (inFlightMessagesWithProducer.isEmpty) completeStage()

          override def onPush(): Unit = {
            val elem: A = grab(in)
            // fetch a jms producer from the pool, and create a holder object to capture the in-flight message.
            val jmsProducer = jmsProducers.dequeue()
            val holder = new Holder(NotYetThere, futureCB, jmsProducer)
            inFlightMessagesWithProducer.enqueue(holder)

            // send the element asynchronously, notifying the holder of (successful or failed) completion.
            Future {
              jmsProducer.send(elem)
              elem
            }.onComplete(holder)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)

            // immediately ask for the next element if producers are available.
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
            case Success(_) => pushNextIfPossible() // on success, try to push out the new element.
            case Failure(ex) => handleFailure(ex, holder)
        }
      )

      private def pullIfNeeded(): Unit =
        if (jmsProducers.nonEmpty && !hasBeenPulled(in)) tryPull(in) // only pull if a producer is available in the pool.

      private def pushNextIfPossible(): Unit =
        if (inFlightMessagesWithProducer.isEmpty) {
          // no messages in flight, are we about to complete?
          if (isClosed(in)) completeStage() else pullIfNeeded()
        } else if (inFlightMessagesWithProducer.peek().elem eq NotYetThere) {
          // next message to be produced is still not there, we need to wait.
          pullIfNeeded()
        } else if (isAvailable(out)) {
          val holder = inFlightMessagesWithProducer.dequeue()
          holder.elem match {
            case Success(elem) =>
              push(out, elem)
              jmsProducers.enqueue(holder.jmsProducer) // put back jms producer to the pool.
              pullIfNeeded() // Ask for the next element.

            case Failure(NonFatal(ex)) => handleFailure(ex, holder)
          }
        }

      private def handleFailure(ex: Throwable, holder: Holder[A]): Unit =
        holder.supervisionDirectiveFor(decider, ex) match {
          case Supervision.Stop => failStage(ex) // fail only if supervision asks for it.
          case _ => pushNextIfPossible()
        }
    }
}

private[jms] object JmsProducerStage {

  val NotYetThere = Failure(new Exception with NoStackTrace)

  /*
   * NOTE: the following code is heavily inspired by akka.stream.impl.fusing.MapAsync
   *
   * To get a condensed view of what the Holder is about, have a look there too.
   */
  class Holder[A <: JmsMessage](var elem: Try[A], val cb: AsyncCallback[Holder[A]], val jmsProducer: JmsMessageProducer)
      extends (Try[A] => Unit) {

    // To support both fail-fast when the supervision directive is Stop
    // and not calling the decider multiple times, we need to cache the decider result and re-use that
    private var cachedSupervisionDirective: OptionVal[Supervision.Directive] = OptionVal.None

    def supervisionDirectiveFor(decider: Supervision.Decider, ex: Throwable): Supervision.Directive =
      cachedSupervisionDirective match {
        case OptionVal.Some(d) => d
        case OptionVal.None =>
          val d = decider(ex)
          cachedSupervisionDirective = OptionVal.Some(d)
          d
      }

    def setElem(t: Try[A]): Unit =
      elem = t match {
        case Success(null) => Failure[A](ReactiveStreamsCompliance.elementMustNotBeNullException)
        case other => other
      }

    override def apply(t: Try[A]): Unit = {
      // invoked on future completion: set the element, and call the stage's completion callback.
      setElem(t)
      cb.invoke(this)
    }
  }
}
