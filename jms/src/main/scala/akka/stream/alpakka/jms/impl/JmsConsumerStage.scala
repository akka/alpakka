/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.impl

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import akka.stream._
import JmsConnector.{JmsConnectorState, JmsConnectorStopping}
import akka.stream.alpakka.jms.{Destination, _}
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.util.OptionVal
import akka.{Done, NotUsed}
import javax.jms._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

private[jms] final class JmsConsumerStage(settings: JmsConsumerSettings, destination: Destination)
    extends GraphStageWithMaterializedValue[SourceShape[Message], JmsConsumerMatValue] {

  private val out = Outlet[Message]("JmsConsumer.out")

  override protected def initialAttributes: Attributes = Attributes.name("JmsConsumer")

  override def shape: SourceShape[Message] = SourceShape[Message](out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, JmsConsumerMatValue) = {
    val logic = new SourceStageLogic[Message](shape, out, settings, destination, inheritedAttributes) {

      private val bufferSize = (settings.bufferSize + 1) * settings.sessionCount

      private val backpressure = new Semaphore(bufferSize)

      protected def createSession(connection: Connection,
                                  createDestination: Session => javax.jms.Destination): JmsConsumerSession = {
        val session =
          connection.createSession(false, settings.acknowledgeMode.getOrElse(AcknowledgeMode.AutoAcknowledge).mode)
        new JmsConsumerSession(connection, session, createDestination(session), destination)
      }

      protected def pushMessage(msg: Message): Unit = {
        push(out, msg)
        backpressure.release()
      }

      override protected def onSessionOpened(jmsSession: JmsConsumerSession): Unit =
        jmsSession
          .createConsumer(settings.selector)
          .map { consumer =>
            consumer.setMessageListener(new MessageListener {
              def onMessage(message: Message): Unit = {
                backpressure.acquire()
                handleMessage.invoke(message)
              }
            })
          }
          .onComplete(sessionOpenedCB.invoke)
    }

    (logic, logic.consumerControl)
  }
}

final class JmsAckSourceStage(settings: JmsConsumerSettings, destination: Destination)
    extends GraphStageWithMaterializedValue[SourceShape[AckEnvelope], JmsConsumerMatValue] {

  private val out = Outlet[AckEnvelope]("JmsSource.out")

  override protected def initialAttributes: Attributes = Attributes.name("JmsAckConsumer")

  override def shape: SourceShape[AckEnvelope] = SourceShape[AckEnvelope](out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, JmsConsumerMatValue) = {

    val logic = new SourceStageLogic[AckEnvelope](shape, out, settings, destination, inheritedAttributes) {
      private val maxPendingAck = settings.bufferSize

      protected def createSession(connection: Connection,
                                  createDestination: Session => javax.jms.Destination): JmsAckSession = {
        val session =
          connection.createSession(false, settings.acknowledgeMode.getOrElse(AcknowledgeMode.ClientAcknowledge).mode)
        new JmsAckSession(connection, session, createDestination(session), destination, settings.bufferSize)
      }

      protected def pushMessage(msg: AckEnvelope): Unit = push(out, msg)

      override protected def onSessionOpened(jmsSession: JmsConsumerSession): Unit =
        jmsSession match {
          case session: JmsAckSession =>
            session
              .createConsumer(settings.selector)
              .map { consumer =>
                consumer.setMessageListener(new MessageListener {

                  var listenerStopped = false

                  def onMessage(message: Message): Unit = {

                    @tailrec
                    def ackQueued(): Unit =
                      OptionVal(session.ackQueue.poll()) match {
                        case OptionVal.Some(action) =>
                          try {
                            action()
                            session.pendingAck -= 1
                          } catch {
                            case _: StopMessageListenerException =>
                              listenerStopped = true
                          }
                          if (!listenerStopped) ackQueued()
                        case OptionVal.None =>
                      }

                    if (!listenerStopped)
                      try {
                        handleMessage.invoke(AckEnvelope(message, session))
                        session.pendingAck += 1
                        if (session.pendingAck > maxPendingAck) {
                          val action = session.ackQueue.take()
                          action()
                          session.pendingAck -= 1
                        }
                        ackQueued()
                      } catch {
                        case _: StopMessageListenerException =>
                          listenerStopped = true
                        case e: JMSException =>
                          handleError.invoke(e)
                      }
                  }
                })
              }
              .onComplete(sessionOpenedCB.invoke)

          case _ =>
            throw new IllegalArgumentException(
              "Session must be of type JMSAckSession, it is a " +
              jmsSession.getClass.getName
            )
        }
    }

    (logic, logic.consumerControl)
  }
}

final class JmsTxSourceStage(settings: JmsConsumerSettings, destination: Destination)
    extends GraphStageWithMaterializedValue[SourceShape[TxEnvelope], JmsConsumerMatValue] {

  private val out = Outlet[TxEnvelope]("JmsSource.out")

  override def shape: SourceShape[TxEnvelope] = SourceShape[TxEnvelope](out)

  override protected def initialAttributes: Attributes = Attributes.name("JmsTxConsumer")

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, JmsConsumerMatValue) = {
    val logic = new SourceStageLogic[TxEnvelope](shape, out, settings, destination, inheritedAttributes) {
      protected def createSession(connection: Connection, createDestination: Session => javax.jms.Destination) = {
        val session =
          connection.createSession(true, settings.acknowledgeMode.getOrElse(AcknowledgeMode.SessionTransacted).mode)
        new JmsConsumerSession(connection, session, createDestination(session), destination)
      }

      protected def pushMessage(msg: TxEnvelope): Unit = push(out, msg)

      override protected def onSessionOpened(jmsSession: JmsConsumerSession): Unit =
        jmsSession match {
          case session: JmsSession =>
            session
              .createConsumer(settings.selector)
              .map { consumer =>
                consumer.setMessageListener(new MessageListener {

                  def onMessage(message: Message): Unit =
                    try {
                      val envelope = TxEnvelope(message, session)
                      handleMessage.invoke(envelope)
                      val action = Await.result(envelope.commitFuture, settings.ackTimeout)
                      action()
                    } catch {
                      case _: TimeoutException => session.session.rollback()
                      case e: IllegalArgumentException => handleError.invoke(e) // Invalid envelope. Fail the stage.
                      case e: JMSException => handleError.invoke(e)
                    }
                })
              }
              .onComplete(sessionOpenedCB.invoke)

          case _ =>
            throw new IllegalArgumentException(
              "Session must be of type JMSAckSession, it is a " +
              jmsSession.getClass.getName
            )
        }
    }

    (logic, logic.consumerControl)
  }
}

abstract class SourceStageLogic[T](shape: SourceShape[T],
                                   out: Outlet[T],
                                   settings: JmsConsumerSettings,
                                   val destination: Destination,
                                   inheritedAttributes: Attributes)
    extends TimerGraphStageLogic(shape)
    with JmsConsumerConnector
    with StageLogging {

  override protected def jmsSettings: JmsConsumerSettings = settings
  private val queue = mutable.Queue[T]()
  private val stopping = new AtomicBoolean(false)
  private var stopped = false

  private val markStopped = getAsyncCallback[Done.type] { _ =>
    stopped = true
    finishStop()
    if (queue.isEmpty) completeStage()
  }

  private val markAborted = getAsyncCallback[Throwable] { ex =>
    stopped = true
    finishStop()
    failStage(ex)
  }

  private[jms] val handleError = getAsyncCallback[Throwable] { e =>
    updateState(JmsConnectorStopping(Failure(e)))
    fail(out, e)
  }

  override def preStart(): Unit = {
    super.preStart()
    ec = executionContext(inheritedAttributes)
    initSessionAsync()
  }

  private[jms] val handleMessage = getAsyncCallback[T] { msg =>
    if (isAvailable(out)) {
      if (queue.isEmpty) {
        pushMessage(msg)
      } else {
        pushMessage(queue.dequeue())
        queue.enqueue(msg)
      }
    } else {
      queue.enqueue(msg)
    }
  }

  protected def pushMessage(msg: T): Unit

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = {
        if (queue.nonEmpty) pushMessage(queue.dequeue())
        if (stopped && queue.isEmpty) completeStage()
      }

      override def onDownstreamFinish(): Unit = {
        // no need to keep messages in the queue, downstream will never pull them.
        queue.clear()
        // keep processing async callbacks for stopSessions.
        setKeepGoing(true)
        stopSessions()
      }
    }
  )

  private def stopSessions(): Unit =
    if (stopping.compareAndSet(false, true)) {
      val status = updateState(JmsConnectorStopping(Success(Done)))
      val connectionFuture = JmsConnector.connection(status)

      val closeSessionFutures = jmsSessions.map { s =>
        val f = s.closeSessionAsync()
        f.failed.foreach(e => log.error(e, "Error closing jms session"))
        f
      }
      Future
        .sequence(closeSessionFutures)
        .onComplete { _ =>
          connectionFuture
            .map { connection =>
              try {
                connection.close()
              } catch {
                case NonFatal(e) => log.error(e, "Error closing JMS connection {}", connection)
              }
            }
            .onComplete { _ =>
              // By this time, after stopping connection, closing sessions, all async message submissions to this
              // stage should have been invoked. We invoke markStopped as the last item so it gets delivered after
              // all JMS messages are delivered. This will allow the stage to complete after all pending messages
              // are delivered, thus preventing message loss due to premature stage completion.
              markStopped.invoke(Done)
            }
        }
    }

  private def abortSessions(ex: Throwable): Unit =
    if (stopping.compareAndSet(false, true)) {
      val status = updateState(JmsConnectorStopping(Failure(ex)))
      val connectionFuture = JmsConnector.connection(status)
      val abortSessionFutures = jmsSessions.map { s =>
        val f = s.abortSessionAsync()
        f.failed.foreach(e => log.error(e, "Error closing jms session"))
        f
      }
      Future
        .sequence(abortSessionFutures)
        .onComplete { _ =>
          connectionFuture
            .map { connection =>
              try {
                connection.close()
                log.info("JMS connection {} closed", connection)
              } catch {
                case NonFatal(e) => log.error(e, "Error closing JMS connection {}", connection)
              }
            }
            .onComplete { _ =>
              markAborted.invoke(ex)
            }
        }
    }

  private[jms] def consumerControl = new JmsConsumerMatValue {
    override def shutdown(): Unit = stopSessions()
    override def abort(ex: Throwable): Unit = abortSessions(ex)
    override def connected: Source[JmsConnectorState, NotUsed] =
      Source.fromFuture(connectionStateSource).flatMapConcat(identity)
  }

  override def postStop(): Unit = finishStop()
}
