/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import akka.{Done, NotUsed}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.annotation.InternalApi
import akka.stream.alpakka.jms._
import akka.stream.impl.Buffer
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.util.OptionVal
import javax.jms

import scala.concurrent.Future
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

/**
 * Internal API.
 */
@InternalApi
private trait JmsProducerConnector extends JmsConnector[JmsProducerSession] {
  this: TimerGraphStageLogic with StageLogging =>

  protected final def createSession(connection: jms.Connection,
                                    createDestination: jms.Session => jms.Destination
  ): JmsProducerSession = {
    val session = connection.createSession(false, AcknowledgeMode.AutoAcknowledge.mode)
    new JmsProducerSession(connection, session, createDestination(session))
  }

  override val startConnection = false

  val status: JmsProducerMatValue = new JmsProducerMatValue {
    override def connected: Source[InternalConnectionState, NotUsed] =
      Source.fromFuture(connectionStateSource).flatMapConcat(identity)
  }
}

/**
 * Internal API.
 */
@InternalApi
private[jms] final class JmsProducerStage[E <: JmsEnvelope[PassThrough], PassThrough](settings: JmsProducerSettings,
                                                                                      destination: Destination
) extends GraphStageWithMaterializedValue[FlowShape[E, E], JmsProducerMatValue] { stage =>
  import JmsProducerStage._

  private val in = Inlet[E]("JmsProducer.in")
  private val out = Outlet[E]("JmsProducer.out")

  override def shape: FlowShape[E, E] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name("JmsProducer") and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, JmsProducerMatValue) = {
    val logic: TimerGraphStageLogic with JmsProducerConnector = producerLogic(inheritedAttributes)
    (logic, logic.status)
  }

  private def producerLogic(inheritedAttributes: Attributes) =
    new TimerGraphStageLogic(shape) with JmsProducerConnector with StageLogging {

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
        super.preStart()
        initSessionAsync()
      }

      override protected def onSessionOpened(jmsSession: JmsProducerSession): Unit =
        sessionOpened(Try {
          jmsProducers.enqueue(JmsMessageProducer(jmsSession, settings, currentJmsProducerEpoch))
          // startup situation: while producer pool was empty, the out port might have pulled. If so, pull from in port.
          // Note that a message might be already in-flight; that's fine since this stage pre-fetches message from
          // upstream anyway to increase throughput once the stream is started.
          if (isAvailable(out)) pullIfNeeded()
        })

      override protected def connectionFailed(ex: Throwable): Unit = {
        jmsProducers.clear()
        currentJmsProducerEpoch += 1
        super.connectionFailed(ex)
      }

      setHandler(out,
                 new OutHandler {
                   override def onPull(): Unit = pushNextIfPossible()

                   override def onDownstreamFinish(): Unit = publishAndCompleteStage()
                 }
      )

      setHandler(
        in,
        new InHandler {
          override def onUpstreamFinish(): Unit = if (inFlightMessages.isEmpty) publishAndCompleteStage()

          override def onUpstreamFailure(ex: Throwable): Unit = {
            publishAndFailStage(ex)
          }

          override def onPush(): Unit = {
            val elem: E = grab(in)
            elem match {
              case _: JmsPassThrough[_] =>
                val holder = new Holder[E](NotYetThere)
                inFlightMessages.enqueue(holder)
                holder(Success(elem))
                pushNextIfPossible()
              case m: JmsEnvelope[_] =>
                // create a holder object to capture the in-flight message, and enqueue it to preserve message order
                val holder = new Holder[E](NotYetThere)
                inFlightMessages.enqueue(holder)
                sendWithRetries(SendAttempt(m.asInstanceOf[E], holder))
            }

            // immediately ask for the next element if producers are available.
            pullIfNeeded()
          }
        }
      )

      private def publishAndCompleteStage(): Unit = {
        val previous = updateState(InternalConnectionState.JmsConnectorStopping(Success(Done)))
        closeSessions()
        closeConnectionAsync(JmsConnector.connection(previous))
        completeStage()
      }

      override def onTimer(timerKey: Any): Unit = timerKey match {
        case s: SendAttempt[E @unchecked] => sendWithRetries(s)
        case _ => super.onTimer(timerKey)
      }

      private def sendWithRetries(send: SendAttempt[E]): Unit = {
        import send._
        if (jmsProducers.nonEmpty) {
          val jmsProducer: JmsMessageProducer = jmsProducers.dequeue()
          Future(jmsProducer.send(envelope)).andThen { case tried =>
            sendCompletedCB.invoke((send, tried, jmsProducer))
          }
        } else {
          nextTryOrFail(send, RetrySkippedOnMissingConnection)
        }
      }

      def nextTryOrFail(send: SendAttempt[E], ex: Throwable): Unit = {
        import send._
        import settings.sendRetrySettings._
        if (maxRetries < 0 || attempt + 1 <= maxRetries) {
          val nextAttempt = attempt + 1
          val delay = if (backoffMaxed) maxBackoff else waitTime(nextAttempt)
          val backoffNowMaxed = backoffMaxed || delay == maxBackoff
          scheduleOnce(send.copy(attempt = nextAttempt, backoffMaxed = backoffNowMaxed), delay)
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
              holder(Success(send.envelope))
              pushNextIfPossible()
            case Failure(t: jms.JMSException) =>
              nextTryOrFail(send, t)
            case Failure(t) =>
              holder(Failure(t))
              handleFailure(t, holder)
          }
      }

      override def postStop(): Unit = finishStop()

      private def pullIfNeeded(): Unit =
        if (
          jmsProducers.nonEmpty // only pull if a producer is available in the pool.
          && !inFlightMessages.isFull // and a place is available in the in-flight queue.
          && !hasBeenPulled(in)
        )
          tryPull(in)

      private def pushNextIfPossible(): Unit =
        if (inFlightMessages.isEmpty) {
          // no messages in flight, are we about to complete?
          if (isClosed(in)) publishAndCompleteStage() else pullIfNeeded()
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

/**
 * Internal API.
 */
@InternalApi
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

  case class SendAttempt[E <: JmsEnvelope[_]](envelope: E,
                                              holder: Holder[E],
                                              attempt: Int = 0,
                                              backoffMaxed: Boolean = false
  )
}
