package akka.stream.alpakka.kinesis.sink

import akka.stream.alpakka.kinesis.KinesisMetrics
import akka.stream.alpakka.kinesis.KinesisMetrics._
import akka.stream.alpakka.kinesis.sink.KinesisSink._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, Inlet, Outlet, Shape}

import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable

private[sink] class ProcessorShape[In: Aggregatable](
    val in: Inlet[In],
    val result: Inlet[Result],
    val out: Outlet[Aggregated]
) extends Shape {
  override def inlets: immutable.Seq[Inlet[_]] = immutable.Seq(in, result)
  override def outlets: immutable.Seq[Outlet[_]] = immutable.Seq(out)
  override def deepCopy(): Shape = new ProcessorShape(in.carbonCopy(), result.carbonCopy(), out.carbonCopy())
}

private[sink] class RecordProcessor[In: Aggregatable](
    shardIndex: Int,
    settings: KinesisSinkSettings,
    aggregatorFactory: Aggregator.Factory[In],
    bufferFactory: Buffer.Factory,
    maxBufferedBytesPerShard: Long,
    maxBufferedBytesTotal: Long,
    totalBufferedBytes: AtomicLong,
    onDrop: Aggregated => Unit
)(implicit metrics: KinesisMetrics)
    extends GraphStage[ProcessorShape[In]] {
  private val inType = implicitly[Aggregatable[In]]

  val in: Inlet[In] = Inlet("in")
  val result: Inlet[Result] = Inlet("result")
  val out: Outlet[Aggregated] = Outlet("out")

  override def shape: ProcessorShape[In] = new ProcessorShape(in, result, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val buffer = bufferFactory()
      private var failure: Option[Throwable] = None
      private val memoryLimit =
        new ShardMemoryLimit(maxBufferedBytesPerShard, maxBufferedBytesTotal, totalBufferedBytes)
      private val aggregator = aggregatorFactory(shardIndex, inType, memoryLimit, settings, metrics)

      private def addToBuffer(elem: Aggregated): Unit = {
        buffer.add(elem)
      }

      private def takeFromBuffer(): Aggregated = {
        val elem = buffer.take()
        memoryLimit.add(-elem.payloadBytes)
        elem
      }

      /** drop data until it's not overflow, or if using the backpressure buffer
       * return whether it's overflow, thus need to backpressure.
       */
      private def dropOrBackpressure(): Boolean = {
        var backpressure = false
        while (!buffer.isEmpty && !backpressure && memoryLimit.shouldDropOrBackpressure) {
          buffer.drop() match {
            case Some(dropped) =>
              memoryLimit.add(-dropped.payloadBytes)
              onDrop(dropped)
            case None =>
              backpressure = memoryLimit.shouldDropOrBackpressure
          }
        }
        backpressure
      }

      private def maybeComplete(): Unit = {
        if (isClosed(in) && buffer.isEmpty && aggregator.isEmpty && pending == 0) {
          failure match {
            case None => completeStage()
            case Some(ex) => failStage(ex)
          }
        }
      }

      private def buildToPush(): Aggregated = {
        if (buffer.isEmpty) addToBuffer(aggregator.getOne())
        val aggregated = takeFromBuffer()
        memoryLimit.sync(metrics)
        aggregated
      }

      override def preStart(): Unit = {
        tryPull(in)
        tryPull(result)
      }

      var pending = 0

      def emit(aggregated: Aggregated): Unit = {
        push(out, aggregated)
        pending += 1
      }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val record = grab(in)
            metrics.record(UserRecordSize, inType.payloadSize(record))
            aggregator.addUserRecord(record) match {
              case util.Failure(_) =>
                metrics.increment(UserRecordTooLarge)
                pull(in)
              case util.Success(aggregated) =>
                aggregated.foreach(addToBuffer)
                if (isAvailable(out)) {
                  emit(buildToPush())
                  pull(in)
                } else if (!dropOrBackpressure()) {
                  pull(in)
                }
            }
          }

          override def onUpstreamFinish(): Unit = maybeComplete()

          override def onUpstreamFailure(ex: Throwable): Unit = {
            failure = Some(ex)
            maybeComplete()
          }
        }
      )

      setHandler(
        result,
        new InHandler {
          override def onPush(): Unit = {
            grab(result) match {
              case Succeeded(record) => ResultStats.onSuccess(record)
              case Failed(record) => ResultStats.onFailure(record)
              case Retryable(record) =>
                ResultStats.onRetryable(record)
                addToBuffer(record)
                memoryLimit.add(record.payloadBytes)
                if (isAvailable(out)) emit(buildToPush())
                else dropOrBackpressure()
            }
            pending -= 1
            maybeComplete()
            tryPull(result)
          }
        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            if (!aggregator.isEmpty || !buffer.isEmpty) emit(buildToPush())
            else maybeComplete()
            if (!hasBeenPulled(in)) tryPull(in)
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            super.onDownstreamFinish(cause)
            while (!buffer.isEmpty) onDrop(takeFromBuffer())
            memoryLimit.sync(metrics)
          }
        }
      )
    }

  private object ResultStats {
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

    def onRetryable(record: Aggregated)(implicit metrics: KinesisMetrics): Unit = {
      metrics.increment(AggRecordRetryable)
      metrics.count(BytesRetryable, record.payloadBytes)
    }
  }
}
