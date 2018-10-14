/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.alpakka.jms.JmsProducerStage._
import akka.stream.alpakka.jms.JmsProducerMessage._
import akka.stream.impl.Buffer
import akka.stream.stage._
import akka.util.OptionVal
import javax.jms.JMSException

import scala.concurrent.Future
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

private[jms] final class JmsProducerStage[A <: JmsMessage, PassThrough](settings: JmsProducerSettings,
                                                                        destination: Destination)
    extends GraphStage[FlowShape[Envelope[A, PassThrough], Envelope[A, PassThrough]]] { stage =>

  private type E = Envelope[A, PassThrough]
  private val in = Inlet[E]("JmsProducer.in")
  private val out = Outlet[E]("JmsProducer.out")

  override def shape: FlowShape[E, E] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with JmsProducerConnector {

      /*
       * NOTE: the following code is heavily inspired by akka.stream.impl.fusing.MapAsync
       *
       * To get a condensed view of what the buffers and handler behavior is about, have a look there too.
       */

      private lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      // the current connection epoch. Reconnects increment this epoch by 1.
      private var currentJmsProducerEpoch = 0

      // available producers for sending messages. Initially full, but might contain less elements if
      // messages are currently in-flight.
      private val jmsProducers: Buffer[JmsMessageProducer] = Buffer(settings.sessionCount, settings.sessionCount)

      // in-flight messages with the producers that were used to send them.
      private val inFlightMessages: Buffer[Holder[E]] =
        Buffer(settings.sessionCount, settings.sessionCount)

      protected val destination: Destination = stage.destination
      protected val jmsSettings: JmsProducerSettings = settings

      override def preStart(): Unit = {
        ec = executionContext(inheritedAttributes)
        initSessionAsync()
      }

      override protected def onSessionOpened(jmsSession: JmsProducerSession): Unit = {
        jmsProducers.enqueue(JmsMessageProducer(jmsSession, settings, currentJmsProducerEpoch))
        // startup situation: while producer pool was empty, the out port might have pulled. If so, pull from in port.
        // Note that a message might be already in-flight; that's fine since this stage pre-fetches message from
        // upstream anyway to increase throughput once the stream is started.
        if (isAvailable(out)) pullIfNeeded()
      }

      override def connectionFailed(ex: Throwable): Unit = {
        jmsProducers.clear()
        currentJmsProducerEpoch += 1
        super.connectionFailed(ex)
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pushNextIfPossible()
      })

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFinish(): Unit = if (inFlightMessages.isEmpty) completeStage()

          override def onPush(): Unit = {
            val elem: E = grab(in)
            elem match {
              case m: Message[_, _] =>
                // create a holder object to capture the in-flight message, and enqueue it to preserve message order
                val holder = new Holder[E](NotYetThere)
                inFlightMessages.enqueue(holder)
                sendWithRetries(SendAttempt[E](m, holder))
              case _: PassThroughMessage[_, _] =>
                val holder = new Holder[E](NotYetThere)
                inFlightMessages.enqueue(holder)
                holder(Success(elem))
                pushNextIfPossible()
            }

            // immediately ask for the next element if producers are available.
            pullIfNeeded()
          }
        }
      )

      override def onTimer(timerKey: Any): Unit = timerKey match {
        case s: SendAttempt[E] => sendWithRetries(s)
        case _ => ()
      }

      private def sendWithRetries(send: SendAttempt[E]): Unit = {
        import send._
        if (jmsProducers.nonEmpty) {
          val jmsProducer: JmsMessageProducer = jmsProducers.dequeue()
          Future(jmsProducer.send(envelope.message)).andThen {
            case tried => sendCompletedCB.invoke((send, tried, jmsProducer))
          }
        } else {
          nextTryOrFail(send, RetrySkippedOnMissingConnection)
        }
      }

      def nextTryOrFail(send: SendAttempt[E], ex: Throwable): Unit = {
        import settings.sendRetrySettings._
        import send._
        if (maxRetries < 0 || attempt + 1 <= maxRetries) {
          val nextAttempt = attempt + 1
          scheduleOnce(send.copy(attempt = nextAttempt), waitTime(nextAttempt))
        } else {
          holder(Failure(ex))
          handleFailure(ex, holder)
        }
      }

      private val sendCompletedCB = getAsyncCallback[(SendAttempt[E], Try[Unit], JmsMessageProducer)] {
        case (send, outcome, jmsProducer) =>
          // same epoch indicates that the producer belongs to the current alive connection.
          if (jmsProducer.epoch == currentJmsProducerEpoch) jmsProducers.enqueue(jmsProducer)

          import send._

          outcome match {
            case Success(_) =>
              holder(Success(envelope))
              pushNextIfPossible()
            case Failure(t: JMSException) =>
              nextTryOrFail(send, t)
            case Failure(t) =>
              holder(Failure(t))
              handleFailure(t, holder)
          }
      }

      override def postStop(): Unit = {
        jmsSessions.foreach(_.closeSession())
        jmsConnection.foreach(_.close)
      }

      private def pullIfNeeded(): Unit =
        if (jmsProducers.nonEmpty // only pull if a producer is available in the pool.
            && !inFlightMessages.isFull // and a place is available in the in-flight queue.
            && !hasBeenPulled(in))
          tryPull(in)

      private def pushNextIfPossible(): Unit =
        if (inFlightMessages.isEmpty) {
          // no messages in flight, are we about to complete?
          if (isClosed(in)) completeStage() else pullIfNeeded()
        } else if (inFlightMessages.peek().elem eq NotYetThere) {
          // next message to be produced is still not there, we need to wait.
          pullIfNeeded()
        } else if (isAvailable(out)) {
          val holder = inFlightMessages.dequeue()
          holder.elem match {
            case Success(elem) =>
              push(out, elem)
              pullIfNeeded() // Ask for the next element.

            case Failure(ex) => handleFailure(ex, holder)
          }
        }

      private def handleFailure(ex: Throwable, holder: Holder[E]): Unit =
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
  class Holder[A](var elem: Try[A]) extends (Try[A] => Unit) {

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

    override def apply(t: Try[A]): Unit = elem = t
  }

  case class SendAttempt[E](envelope: Message[JmsMessage, _] with E, holder: Holder[E], attempt: Int = 0)
}
