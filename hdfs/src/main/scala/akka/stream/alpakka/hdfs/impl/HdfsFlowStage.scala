/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl

import akka.NotUsed
import akka.event.Logging
import akka.stream.alpakka.hdfs._
import akka.stream.alpakka.hdfs.impl.HdfsFlowLogic.{FlowState, FlowStep, LogicState}
import akka.stream.alpakka.hdfs.impl.strategy.RotationStrategy.TimeRotationStrategy
import akka.stream.alpakka.hdfs.impl.strategy.{RotationStrategy, SyncStrategy}
import akka.stream.alpakka.hdfs.impl.writer.HdfsWriter
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import cats.data.State

/**
 * Internal API
 */
private[hdfs] final class HdfsFlowStage[W, I, C](
    ss: SyncStrategy,
    rs: RotationStrategy,
    settings: HdfsWritingSettings,
    hdfsWriter: HdfsWriter[W, I]
) extends GraphStage[FlowShape[IncomingMessage[I, C], OutgoingMessage[C]]] {

  private val in = Inlet[IncomingMessage[I, C]](Logging.simpleName(this) + ".in")
  private val out = Outlet[OutgoingMessage[C]](Logging.simpleName(this) + ".out")

  override val shape: FlowShape[IncomingMessage[I, C], OutgoingMessage[C]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new HdfsFlowLogic(ss, rs, settings, hdfsWriter, in, out, shape)
}

/**
 * Internal API
 */
private final class HdfsFlowLogic[W, I, C](
    initialSyncStrategy: SyncStrategy,
    initialRotationStrategy: RotationStrategy,
    settings: HdfsWritingSettings,
    initialHdfsWriter: HdfsWriter[W, I],
    inlet: Inlet[IncomingMessage[I, C]],
    outlet: Outlet[OutgoingMessage[C]],
    shape: FlowShape[IncomingMessage[I, C], OutgoingMessage[C]]
) extends TimerGraphStageLogic(shape)
    with InHandler
    with OutHandler {

  private var state = FlowState(initialHdfsWriter, initialRotationStrategy, initialSyncStrategy)

  private val flushProgram = rotateOutput.flatMap(message => tryPush(Seq(message)))

  setHandlers(inlet, outlet, this)

  def onPush(): Unit =
    state = onPushProgram(grab(inlet))
      .runS(state)
      .value

  def onPull(): Unit =
    tryPull()

  override def preStart(): Unit = {
    // Schedule timer to rotate output file
    initialRotationStrategy match {
      case timed: TimeRotationStrategy =>
        schedulePeriodicallyWithInitialDelay(NotUsed, timed.interval, timed.interval)
      case _ => ()
    }
    tryPull()
  }

  override def onTimer(timerKey: Any): Unit =
    state = flushProgram.runS(state).value

  override def onUpstreamFailure(ex: Throwable): Unit =
    failStage(ex)

  override def onUpstreamFinish(): Unit =
    state.logicState match {
      case LogicState.Writing =>
        flushProgram
          .run(state)
          .map(_ => completeStage())
          .value
      case _ => completeStage()
    }

  private def tryPull(): Unit =
    if (!isClosed(inlet) && !hasBeenPulled(inlet)) {
      pull(inlet)
    }

  private def onPushProgram(input: IncomingMessage[I, C]) =
    for {
      _ <- setLogicState(LogicState.Writing)
      offset <- write(input.source)
      _ <- updateSync(offset)
      _ <- updateRotation(offset)
      _ <- trySyncOutput
      rotationResult <- tryRotateOutput
      (rotationCount, maybeRotationMessage) = rotationResult
      messages = Seq(Some(WrittenMessage(input.passThrough, rotationCount)), maybeRotationMessage)
      _ <- tryPush(messages.flatten)
    } yield tryPull()

  private def setLogicState(logicState: LogicState): FlowStep[W, I, LogicState] =
    FlowStep[W, I, LogicState] { state =>
      (state.copy(logicState = logicState), logicState)
    }

  private def write(input: I): FlowStep[W, I, Long] =
    FlowStep[W, I, Long] { state =>
      val newOffset = state.writer.write(input, settings.newLine)
      (state, newOffset)
    }

  private def updateRotation(offset: Long): FlowStep[W, I, RotationStrategy] =
    FlowStep[W, I, RotationStrategy] { state =>
      val newRotation = state.rotationStrategy.update(offset)
      (state.copy(rotationStrategy = newRotation), newRotation)
    }

  private def updateSync(offset: Long): FlowStep[W, I, SyncStrategy] =
    FlowStep[W, I, SyncStrategy] { state =>
      val newSync = state.syncStrategy.update(offset)
      (state.copy(syncStrategy = newSync), newSync)
    }

  private def rotateOutput: FlowStep[W, I, RotationMessage] =
    FlowStep[W, I, RotationMessage] { state =>
      val newRotationCount = state.rotationCount + 1
      val newRotation = state.rotationStrategy.reset()
      val newWriter = state.writer.rotate(newRotationCount)

      state.writer.moveToTarget()

      val message = RotationMessage(state.writer.targetFileName, state.rotationCount)
      val newState = state.copy(rotationCount = newRotationCount,
                                writer = newWriter,
                                rotationStrategy = newRotation,
                                logicState = LogicState.Idle)

      (newState, message)
    }

  /*
    It tries to rotate output file.
    If it rotates, it returns previous rotation count and a message,
    else, it returns current rotation without a message.
   */
  private def tryRotateOutput: FlowStep[W, I, (Int, Option[RotationMessage])] =
    FlowStep[W, I, (Int, Option[RotationMessage])] { state =>
      if (state.rotationStrategy.should()) {
        rotateOutput
          .run(state)
          .map {
            case (newState, message) =>
              (newState, (state.rotationCount, Some(message)))
          }
          .value
      } else {
        (state, (state.rotationCount, None))
      }
    }

  private def trySyncOutput: FlowStep[W, I, Boolean] =
    FlowStep[W, I, Boolean] { state =>
      if (state.syncStrategy.should()) {
        state.writer.sync()
        val newSync = state.syncStrategy.reset()
        (state.copy(syncStrategy = newSync), true)
      } else {
        (state, false)
      }
    }

  private def tryPush(messages: Seq[OutgoingMessage[C]]): FlowStep[W, I, Unit] =
    FlowStep[W, I, Unit] { state =>
      if (messages.nonEmpty)
        emitMultiple(outlet, messages.toIterator)
      (state, ())
    }

}

private object HdfsFlowLogic {

  type FlowStep[W, I, A] = State[FlowState[W, I], A]
  object FlowStep {
    def apply[W, I, A](f: FlowState[W, I] => (FlowState[W, I], A)): FlowStep[W, I, A] = State.apply(f)
  }

  sealed trait LogicState
  object LogicState {
    case object Idle extends LogicState
    case object Writing extends LogicState
  }

  final case class FlowState[W, I](
      rotationCount: Int,
      writer: HdfsWriter[W, I],
      rotationStrategy: RotationStrategy,
      syncStrategy: SyncStrategy,
      logicState: LogicState
  )

  object FlowState {
    def apply[W, I](
        writer: HdfsWriter[W, I],
        rs: RotationStrategy,
        ss: SyncStrategy
    ): FlowState[W, I] = new FlowState[W, I](0, writer, rs, ss, LogicState.Idle)
  }
}
