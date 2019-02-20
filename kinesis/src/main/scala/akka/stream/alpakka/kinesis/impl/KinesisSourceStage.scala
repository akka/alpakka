/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.impl

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.stream.alpakka.kinesis.{ShardSettings, KinesisErrors => Errors}
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model._

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Internal API
 */
@InternalApi
private[kinesis] object KinesisSourceStage {

  private[kinesis] final case class GetShardIteratorSuccess(result: GetShardIteratorResult)

  private[kinesis] final case class GetShardIteratorFailure(ex: Exception)

  private[kinesis] final case class GetRecordsSuccess(records: GetRecordsResult)

  private[kinesis] final case class GetRecordsFailure(ex: Exception)

  private[kinesis] final case object Pump

}

/**
 * Internal API
 */
@InternalApi
private[kinesis] class KinesisSourceStage(shardSettings: ShardSettings, amazonKinesisAsync: => AmazonKinesisAsync)
    extends GraphStage[SourceShape[Record]] {

  import KinesisSourceStage._

  private val out = Outlet[Record]("Records")

  override def shape: SourceShape[Record] = new SourceShape[Record](out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging {

      import shardSettings._

      private[this] var currentShardIterator: String = _
      private[this] val buffer = mutable.Queue.empty[Record]
      private[this] var self: StageActor = _

      override def preStart(): Unit = {
        self = getStageActor(awaitingShardIterator)
        requestShardIterator()
      }

      setHandler(shape.out, new OutHandler {
        override def onPull(): Unit = self.ref ! Pump
      })

      private def awaitingShardIterator(in: (ActorRef, Any)): Unit = in match {
        case (_, GetShardIteratorSuccess(result)) =>
          currentShardIterator = result.getShardIterator
          self.become(awaitingRecords)
          requestRecords()

        case (_, GetShardIteratorFailure(ex)) =>
          log.error(ex, "Failed to get a shard iterator for shard {}", shardId)
          failStage(new Errors.GetShardIteratorError(shardId, ex))

        case (_, Pump) =>
        case (_, msg) =>
          throw new IllegalArgumentException(s"unexpected message $msg in state `ready`")
      }

      private def awaitingRecords(in: (ActorRef, Any)): Unit = in match {
        case (_, GetRecordsSuccess(result)) =>
          val records = result.getRecords.asScala
          if (result.getNextShardIterator == null) {
            log.info("Shard {} returned a null iterator and will now complete.", shardId)
            completeStage()
          } else {
            currentShardIterator = result.getNextShardIterator
          }
          if (records.nonEmpty) {
            records.foreach(buffer.enqueue(_))
            self.become(ready)
            self.ref ! Pump
          } else {
            scheduleOnce('GET_RECORDS, refreshInterval)
          }

        case (_, GetRecordsFailure(ex)) =>
          log.error(ex, "Failed to fetch records from Kinesis for shard {}", shardId)
          failStage(new Errors.GetRecordsError(shardId, ex))

        case (_, Pump) =>
        case (_, msg) =>
          throw new IllegalArgumentException(s"unexpected message $msg in state `ready`")
      }

      private def ready(in: (ActorRef, Any)): Unit = in match {
        case (_, Pump) =>
          if (isAvailable(shape.out)) {
            push(shape.out, buffer.dequeue())
            self.ref ! Pump
          }
          if (buffer.isEmpty) {
            self.become(awaitingRecords)
            requestRecords()
          }

        case (_, msg) =>
          throw new IllegalArgumentException(s"unexpected message $msg in state `ready`")
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case 'GET_RECORDS => requestRecords()
      }

      private[this] val handleGetRecords =
        new AsyncHandler[GetRecordsRequest, GetRecordsResult] {
          override def onSuccess(request: GetRecordsRequest, result: GetRecordsResult): Unit =
            self.ref ! GetRecordsSuccess(result)
          override def onError(exception: Exception): Unit = self.ref ! GetRecordsFailure(exception)
        }

      private[this] def requestRecords(): Unit =
        amazonKinesisAsync.getRecordsAsync(
          new GetRecordsRequest().withLimit(limit).withShardIterator(currentShardIterator),
          handleGetRecords
        )

      private[this] def requestShardIterator(): Unit = {
        val request = Function.chain[GetShardIteratorRequest](
          Seq(
            r => startingSequenceNumber.fold(r)(r.withStartingSequenceNumber),
            r => atTimestamp.fold(r)(instant => r.withTimestamp(java.util.Date.from(instant)))
          )
        )(
          new GetShardIteratorRequest()
            .withStreamName(streamName)
            .withShardId(shardId)
            .withShardIteratorType(shardIteratorType)
        )
        val handleShardIterator =
          new AsyncHandler[GetShardIteratorRequest, GetShardIteratorResult] {
            override def onSuccess(request: GetShardIteratorRequest, result: GetShardIteratorResult): Unit =
              self.ref ! GetShardIteratorSuccess(result)
            override def onError(exception: Exception): Unit = self.ref ! GetShardIteratorFailure(exception)
          }
        amazonKinesisAsync.getShardIteratorAsync(request, handleShardIterator)
      }

    }

}
