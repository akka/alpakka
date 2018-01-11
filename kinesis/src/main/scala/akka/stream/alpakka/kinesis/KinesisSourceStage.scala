/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.concurrent.Future

import akka.actor.ActorRef
import akka.stream.alpakka.kinesis.KinesisSourceStage._
import akka.stream.alpakka.kinesis.{KinesisErrors => Errors}
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model._

import scala.collection.JavaConverters._
import scala.collection.mutable

object KinesisSourceStage {

  private[kinesis] final case class GetShardIteratorSuccess(result: GetShardIteratorResult)

  private[kinesis] final case class GetShardIteratorFailure(ex: Throwable)

  private[kinesis] final case class GetRecordsSuccess(records: GetRecordsResult)

  private[kinesis] final case class GetRecordsFailure(ex: Throwable)

  private[kinesis] final case object Pump

}

class KinesisSourceStage(shardSettings: ShardSettings, amazonKinesisAsync: => AmazonKinesisAsync)
    extends GraphStage[SourceShape[Record]] {

  private val out = Outlet[Record]("Records")

  override def shape: SourceShape[Record] = new SourceShape[Record](out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging {

      import shardSettings._

      private[this] var iterator: String = _
      private[this] var buffer = mutable.Queue.empty[Record]
      private[this] var self: StageActor = _

      override def preStart(): Unit = {
        self = getStageActor(awaitingShardIterator)
        getShardIterator()
      }

      setHandler(shape.out, new OutHandler {
        override def onPull(): Unit = self.ref ! Pump
      })

      private def awaitingShardIterator(in: (ActorRef, Any)): Unit = in match {
        case ((_, GetShardIteratorSuccess(result))) => {
          iterator = result.getShardIterator
          self.become(awaitingRecords)
          requestRecords(self.ref)
        }
        case ((_, GetShardIteratorFailure(_))) => {
          log.error("Failed to get a shard iterator for shard {}", shardId)
          failStage(Errors.GetShardIteratorError)
        }
        case ((_, Pump)) => ()
      }

      private def awaitingRecords(in: (ActorRef, Any)): Unit = in match {
        case ((_, GetRecordsSuccess(result))) => {
          val records = result.getRecords.asScala
          Option(result.getNextShardIterator).fold {
            log.info("Shard {} returned a null iterator and will now complete.", shardId)
            completeStage()
          } {
            iterator = _
          }
          if (records.nonEmpty) {
            records.foreach(buffer.enqueue(_))
            self.become(ready)
            self.ref ! Pump
          } else {
            scheduleOnce('GET_RECORDS, refreshInterval)
          }
        }
        case ((_, GetRecordsFailure(_))) => {
          log.error("Failed to fetch records from Kinesis for shard {}", shardId)
          failStage(Errors.GetRecordsError)
        }
        case ((_, Pump)) => ()
      }

      private def ready(in: (ActorRef, Any)): Unit = in match {
        case ((_, Pump)) => {
          if (isAvailable(shape.out)) {
            push(shape.out, buffer.dequeue())
            self.ref ! Pump
          }
          if (buffer.isEmpty) {
            self.become(awaitingRecords)
            requestRecords(self.ref)
          }
        }
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case 'GET_RECORDS => requestRecords(self.ref)
      }

      private[this] def getRecordsHandler(ref: ActorRef) =
        getAmazonKinesisHandler[GetRecordsRequest, GetRecordsResult](
          (result: GetRecordsResult) => self.ref ! GetRecordsSuccess(result),
          (ex: Throwable) => self.ref ! GetRecordsFailure(ex)
        )

      private[this] def requestRecords(ref: ActorRef): Unit =
        amazonKinesisAsync.getRecordsAsync(new GetRecordsRequest().withLimit(limit).withShardIterator(iterator),
                                           getRecordsHandler(self.ref))

      private[this] def getShardIteratorHandler(ref: ActorRef) =
        getAmazonKinesisHandler[GetShardIteratorRequest, GetShardIteratorResult](
          (result: GetShardIteratorResult) => ref ! GetShardIteratorSuccess(result),
          (ex: Throwable) => ref ! GetShardIteratorFailure(ex)
        )

      private[this] def getShardIterator(): Future[GetShardIteratorResult] = {
        val request = Function.chain[GetShardIteratorRequest](
          Seq(
            _.withStreamName(streamName),
            _.withShardId(shardId),
            _.withShardIteratorType(shardIteratorType),
            r => startingSequenceNumber.fold(r)(r.withStartingSequenceNumber),
            r => atTimestamp.fold(r)(r.withTimestamp)
          )
        )(new GetShardIteratorRequest())
        amazonKinesisAsync.getShardIteratorAsync(request, getShardIteratorHandler(self.ref))
      }

      def getAmazonKinesisHandler[I <: AmazonWebServiceRequest, O](
          successCallback: O => Unit,
          errorCallback: Throwable => Unit
      ): AsyncHandler[I, O] =
        new AsyncHandler[I, O] {
          override def onError(exception: Exception): Unit = errorCallback(exception)
          override def onSuccess(request: I, result: O): Unit = successCallback(result)
        }

    }

}
