/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.impl

import java.util.concurrent.Semaphore

import akka.annotation.InternalApi
import akka.stream.alpakka.kinesis.KinesisSchedulerErrors.SchedulerUnexpectedShutdown
import akka.stream.alpakka.kinesis.{CommittableRecord, KinesisSchedulerSourceSettings}
import akka.stream.stage._
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.processor.{ShardRecordProcessor, ShardRecordProcessorFactory}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Internal API
 */
@InternalApi
private[kinesis] object KinesisSchedulerSourceStage {

  sealed trait Command
  final case class NewRecord(cr: CommittableRecord) extends Command
  final case object Pump extends Command
  final case object Complete extends Command
  final case class SchedulerShutdown(result: Try[_]) extends Command

}

/**
 * Internal API
 */
@InternalApi
private[kinesis] class KinesisSchedulerSourceStage(
    settings: KinesisSchedulerSourceSettings,
    schedulerBuilder: ShardRecordProcessorFactory => Scheduler
)(implicit ec: ExecutionContext)
    extends GraphStageWithMaterializedValue[SourceShape[CommittableRecord], Future[Scheduler]] {

  private val out = Outlet[CommittableRecord]("Records")
  override def shape: SourceShape[CommittableRecord] = new SourceShape[CommittableRecord](out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Scheduler]) = {
    val matValue = Promise[Scheduler]()
    new Logic(matValue) -> matValue.future
  }

  final class Logic(matValue: Promise[Scheduler]) extends GraphStageLogic(shape) with StageLogging with OutHandler {
    setHandler(out, this)

    import KinesisSchedulerSourceStage._
    import settings._

    // We're transmitting backpressure from the Outlet to the Scheduler using a Semaphore instance
    // semaphore.acquire ~> callback ~> push downstream ~> semaphore.release
    private[this] val backpressureSemaphore = new Semaphore(bufferSize)
    private[this] val buffer = mutable.Queue.empty[CommittableRecord]
    private[this] var schedulerOpt: Option[Scheduler] = None

    override def preStart(): Unit = {
      val scheduler = schedulerBuilder(new ShardRecordProcessorFactory {
        override def shardRecordProcessor(): ShardRecordProcessor =
          new ShardProcessor(newRecordCallback)
      })
      schedulerOpt = Some(scheduler)
      Future(scheduler.run()).onComplete(result => callback.invoke(SchedulerShutdown(result)))
      matValue.success(scheduler)
    }
    private val callback: AsyncCallback[Command] = getAsyncCallback(awaitingRecords)
    private def newRecordCallback(record: CommittableRecord): Unit = {
      backpressureSemaphore.tryAcquire(backpressureTimeout.length, backpressureTimeout.unit)
      callback.invoke(NewRecord(record))
    }
    override def onPull(): Unit = awaitingRecords(Pump)
    override def onDownstreamFinish(cause: Throwable): Unit = awaitingRecords(Complete)
    @tailrec
    private def awaitingRecords(in: Command): Unit = in match {
      case NewRecord(record) =>
        buffer.enqueue(record)
        awaitingRecords(Pump)
      case Pump =>
        if (isAvailable(shape.out) && buffer.nonEmpty) {
          push(shape.out, buffer.dequeue())
          backpressureSemaphore.release()
          awaitingRecords(Pump)
        }
      case SchedulerShutdown(Success(_)) | Complete =>
        buffer.clear()
        completeStage()
      case SchedulerShutdown(Failure(e)) =>
        buffer.clear()
        failStage(SchedulerUnexpectedShutdown(e))
    }
    override def postStop(): Unit =
      schedulerOpt.foreach(scheduler => Future(if (!scheduler.shutdownComplete()) scheduler.shutdown()))
  }
}
