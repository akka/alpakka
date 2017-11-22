/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.scaladsl

import akka.NotUsed
import akka.stream.Supervision.{Resume, Stop}
import akka.stream.alpakka.kinesis.worker.CommittableRecord
import akka.stream.alpakka.kinesis.{
  KinesisWorkerCheckpointSettings,
  KinesisWorkerSourceSettings,
  KinesisWorkerSourceStage
}
import akka.stream.{scaladsl, ActorAttributes, FlowShape}
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source, Zip}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.model.Record

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

object KinesisWorker {

  def apply(
      workerBuilder: IRecordProcessorFactory => Worker,
      settings: KinesisWorkerSourceSettings = KinesisWorkerSourceSettings.defaultInstance
  )(implicit workerExecutor: ExecutionContext): Source[CommittableRecord, NotUsed] =
    Source.fromGraph(new KinesisWorkerSourceStage(settings.bufferSize, settings.checkWorkerPeriodicity, workerBuilder))

  def checkpointRecordsFlow(
      settings: KinesisWorkerCheckpointSettings = KinesisWorkerCheckpointSettings.defaultInstance
  ): Flow[CommittableRecord, Record, NotUsed] =
    Flow[CommittableRecord]
      .groupBy(MAX_KINESIS_SHARDS, _.shardId)
      .groupedWithin(settings.maxBatchSize, settings.maxBatchWait)
      .via(GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._

        val `{` = b.add(scaladsl.Broadcast[immutable.Seq[CommittableRecord]](2))
        val `}` = b.add(Zip[Unit, immutable.Seq[CommittableRecord]])
        val `=` = b.add(Flow[Record])

        `{`.out(0)
          .map(_.max)
          .mapAsync(1)(r => if (r.canBeCheckpointed()) r.checkpoint() else Future.successful(())) ~> `}`.in0
        `{`.out(1) ~> `}`.in1

        `}`.out.map(_._2).mapConcat(identity).map(_.record) ~> `=`

        FlowShape(`{`.in, `=`.out)
      })
      .mergeSubstreams
      .withAttributes(ActorAttributes.supervisionStrategy {
        case _: com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException => Resume
        case _ => Stop
      })

  def checkpointRecordsSink(
      settings: KinesisWorkerCheckpointSettings = KinesisWorkerCheckpointSettings.defaultInstance
  ): Sink[CommittableRecord, NotUsed] =
    checkpointRecordsFlow(settings).to(Sink.ignore)

  // http://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
  private val MAX_KINESIS_SHARDS = 500

}
