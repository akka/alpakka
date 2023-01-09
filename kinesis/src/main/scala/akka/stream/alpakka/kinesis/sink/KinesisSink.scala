package akka.stream.alpakka.kinesis.sink

import akka.dispatch.ExecutionContexts.parasitic
import akka.stream._
import akka.stream.alpakka.kinesis.KinesisLimits._
import akka.stream.alpakka.kinesis.KinesisMetrics
import akka.stream.alpakka.kinesis.KinesisMetrics._
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition, Sink}
import akka.{Done, NotUsed}
import software.amazon.awssdk.core.exception.{
  AbortedException,
  ApiCallAttemptTimeoutException,
  ApiCallTimeoutException,
  SdkClientException
}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  KinesisException,
  ListShardsRequest,
  PutRecordsRequest,
  PutRecordsResultEntry
}

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CompletionException, TimeoutException}
import scala.annotation.tailrec
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object KinesisSink {

  private object Stats {
    def onBatch(batch: Iterable[Aggregated])(implicit metrics: KinesisMetrics): Unit = {
      val totalBytes = batch.map(_.payloadBytes).sum
      metrics.record(BatchRequestBytes, totalBytes)
      metrics.count(BytesSent, totalBytes)
    }

    def onSuccess(record: Aggregated)(implicit metrics: KinesisMetrics): Unit = {
      metrics.increment(AggRecordSuccess, record.group.tags)
      metrics.count(UserRecordSuccess, record.aggregatedRecords, record.group.tags)
      metrics.count(BytesSuccess, record.payloadBytes, record.group.tags)
    }

    def onFailure(record: Aggregated)(implicit metrics: KinesisMetrics): Unit = {
      metrics.increment(AggRecordFailure, record.group.tags)
      metrics.count(UserRecordFailure, record.aggregatedRecords, record.group.tags)
      metrics.count(BytesFailure, record.payloadBytes, record.group.tags)
    }

    def onDrop(record: Aggregated)(implicit metrics: KinesisMetrics): Unit = {
      metrics.increment(AggRecordDropped, record.group.tags)
      metrics.count(UserRecordDropped, record.aggregatedRecords, record.group.tags)
      metrics.count(BytesDropped, record.payloadBytes, record.group.tags)
    }

    def onRetryable(record: Aggregated)(implicit metrics: KinesisMetrics): Unit = {
      metrics.increment(AggRecordRetryable)
      metrics.count(BytesRetryable, record.payloadBytes)
    }
  }

  private[sink] trait Result {
    def record: Aggregated
  }
  private[sink] case class Succeeded(record: Aggregated) extends Result
  private[sink] case class Retryable(record: Aggregated) extends Result
  private[sink] case class Failed(record: Aggregated) extends Result

  trait PutRecordsHandler {
    def handleResult(request: Aggregated, result: PutRecordsResultEntry): Result
    def handleError(requests: Iterable[Aggregated], error: Throwable): Iterable[Result]
  }

  private object DefaultPutRecordsHandler extends PutRecordsHandler {
    override def handleResult(request: Aggregated, result: PutRecordsResultEntry): Result = {
      if (result.shardId() != null) Succeeded(request)
      else Retryable(request)
    }

    @tailrec
    override def handleError(requests: Iterable[Aggregated], error: Throwable): Iterable[Result] = {
      error match {
        case ex: CompletionException => handleError(requests, ex.getCause)
        case ex if isRetryable(ex) => requests.map(Retryable)
        case _ => requests.map(Failed)
      }
    }

    protected def isRetryable(throwable: Throwable): Boolean = throwable match {
      case _: KinesisException | _: AbortedException | _: ApiCallAttemptTimeoutException | _: ApiCallTimeoutException =>
        true
      case ex: SdkClientException if isTimeoutException(ex) => true
      case _ => false
    }

    @tailrec
    private[this] def isTimeoutException(ex: Throwable): Boolean = ex.getCause match {
      case cause if cause == null => false
      case cause if cause.isInstanceOf[TimeoutException] => true
      case cause => isTimeoutException(cause)
    }
  }

  type SinkType[T] = Sink[T, Future[Done]]
  type PutRecords = Flow[Iterable[Aggregated], Result, NotUsed]

  def apply[In: Aggregatable](
      settings: KinesisSinkSettings,
      aggregator: Aggregator.Factory[In] = Aggregator.default[In],
      buffer: Buffer.Factory = Buffer.backpressure,
      handler: PutRecordsHandler = DefaultPutRecordsHandler
  )(implicit kinesisClient: KinesisAsyncClient, metrics: KinesisMetrics): SinkType[In] = {
    def putRecords =
      Flow[Iterable[Aggregated]]
        .mapAsyncUnordered(settings.maxConcurrentRequests) { records =>
          val request = PutRecordsRequest
            .builder()
            .streamName(settings.streamName)
            .records(records.map(_.request).asJavaCollection)
            .build()

          kinesisClient
            .putRecords(request)
            .toScala
            .map(
              _.records().asScala
                .zip(records)
                .map {
                  case (result, request) =>
                    handler.handleResult(request, result)
                }
            )(parasitic)
            .recover(PartialFunction.fromFunction(handler.handleError(records, _)))(parasitic)
        }
        .mapConcat(identity)

    Sink
      .futureSink(
        getSharding(settings.streamName)
          .map(apply(settings, aggregator, buffer, _, putRecords))(parasitic)
      )
      .mapMaterializedValue(_.flatten)
  }

  private[sink] def apply[In: Aggregatable](
      settings: KinesisSinkSettings,
      aggregatorFactory: Aggregator.Factory[In],
      bufferFactory: Buffer.Factory,
      sharding: Sharding,
      putRecords: PutRecords
  )(implicit metrics: KinesisMetrics): SinkType[In] = {

    val inType = implicitly[Aggregatable[In]]
    val ignore = new DropOnClose[Aggregated](record => Stats.onDrop(record))

    val maxRecordsPerSecondPerShard = MaxRecordsPerSecondPerShard / settings.nrOfInstance
    val maxBytesPerSecondPerShard = MaxBytesPerSecondPerShard / settings.nrOfInstance

    val partitioner = (in: In) => {
      val explicitHashKey = inType.getExplicitHashKey(in).getOrElse(Sharding.createExplicitHashKey(inType.getKey(in)))
      sharding.getShard(explicitHashKey)
    }

    val maxBurstBytes = (maxBytesPerSecondPerShard * settings.maxBurstDuration / 1.second).toInt
    val maxBurstRecords = (maxRecordsPerSecondPerShard * settings.maxBurstDuration / 1.second).toInt
    val maxBatchRequestBytes = settings.maxBatchRequestBytes
    val maxBufferedBytes = Seq(
      settings.maxBufferedBytes.map(_ / sharding.nrOfShards),
      settings.maxBufferedDuration.map(d => (d / 1.second * MaxBytesPerSecondPerShard).toInt)
    ).flatten.min

    Sink.fromGraph(GraphDSL.createGraph(ignore) { implicit b: GraphDSL.Builder[Future[Done]] => ignore =>
      import GraphDSL.Implicits._

      val inPart = b.add(Partition[In](sharding.nrOfShards, partitioner))

      val retryPart = b.add(Partition[Aggregated](sharding.nrOfShards, _.shardIndex))

      val merge = b.add(Merge[Aggregated](sharding.nrOfShards))

      val viaPutRecords = b.add(
        Flow[Aggregated]
        // .groupedWeighted(maxBatchRequestBytes)(_.payloadBytes)
          .groupedWeightedWithin(maxBatchRequestBytes, 100.millis)(_.payloadBytes)
          .addAttributes(Attributes.asyncBoundary and Attributes.inputBuffer(1, 1))
          .alsoTo(Sink.foreach(Stats.onBatch))
          .via(putRecords)
          .divertTo(
            Sink.foreach(
              result =>
                (result: @unchecked) match {
                  case Succeeded(record) => Stats.onSuccess(record)
                  case Failed(record) => Stats.onFailure(record)
                }
            ),
            result => !result.isInstanceOf[Retryable]
          )
          .collect {
            case Retryable(record) =>
              Stats.onRetryable(record)
              record
          }
      )

      val totalBufferedBytes = new AtomicLong(0)

      for (shardIndex <- 0 until sharding.nrOfShards) {
        val processor = b.add(
          new RecordProcessor(
            shardIndex = shardIndex,
            settings = settings,
            aggregatorFactory = aggregatorFactory,
            bufferFactory = bufferFactory,
            maxBufferedBytesPerShard = maxBufferedBytes,
            maxBufferedBytesTotal = maxBufferedBytes * sharding.nrOfShards,
            totalBufferedBytes = totalBufferedBytes,
            onDrop = Stats.onDrop
          )
        )

        val throttle = b.add {
          Flow[Aggregated]
            .throttle(maxRecordsPerSecondPerShard, 1.second, maxBurstRecords, ThrottleMode.Shaping)
            .throttle(maxBytesPerSecondPerShard, 1.second, maxBurstBytes, _.payloadBytes, ThrottleMode.Shaping)
            .async
        }

        inPart.out(shardIndex) ~> processor.in
        retryPart.out(shardIndex) ~> processor.retry
        processor.out ~> throttle ~> merge.in(shardIndex)
      }

      merge ~> viaPutRecords ~> ignore ~> retryPart.in

      SinkShape(inPart.in)
    })
  }

  private def getSharding(streamName: String)(implicit client: KinesisAsyncClient): Future[Sharding] = {
    client
      .listShards(ListShardsRequest.builder().streamName(streamName).build())
      .toScala
      .map(resp => Sharding(resp.shards().asScala))(parasitic)
  }
}
