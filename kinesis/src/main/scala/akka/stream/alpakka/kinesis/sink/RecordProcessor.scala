package akka.stream.alpakka.kinesis.sink

import akka.stream.alpakka.kinesis.KinesisMetrics
import akka.stream.alpakka.kinesis.KinesisMetrics._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, Inlet, Outlet, Shape}

import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable

private[sink] class ProcessorShape[In: Aggregatable](
    val in: Inlet[In],
    val retry: Inlet[Aggregated],
    val out: Outlet[Aggregated]
) extends Shape {
  override def inlets: immutable.Seq[Inlet[_]] = immutable.Seq(in, retry)
  override def outlets: immutable.Seq[Outlet[_]] = immutable.Seq(out)
  override def deepCopy(): Shape = new ProcessorShape(in.carbonCopy(), retry.carbonCopy(), out.carbonCopy())
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
  val retry: Inlet[Aggregated] = Inlet("retry")
  val out: Outlet[Aggregated] = Outlet("out")

  override def shape: ProcessorShape[In] = new ProcessorShape(in, retry, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val buffer = bufferFactory()
      private var failure: Option[Throwable] = None
      private val memoryLimit =
        new ShardMemoryLimit(maxBufferedBytesPerShard, maxBufferedBytesTotal, totalBufferedBytes)
      private val aggregator = aggregatorFactory(shardIndex, inType, memoryLimit, settings, metrics)

      private def addToBuffer(elem: Aggregated): Unit = {
        buffer.add(elem)
        // memoryLimit.add(elem.payloadBytes)
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
        if (isClosed(in) && buffer.isEmpty && aggregator.isEmpty) {
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
                  push(out, buildToPush())
                  pull(in)
                } else if (!dropOrBackpressure()) {
                  pull(in)
                }
            }
          }

          override def onUpstreamFinish(): Unit = {
            if (!isClosed(retry)) cancel(retry)
            maybeComplete()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            if (!isClosed(retry)) cancel(retry)
            failure = Some(ex)
            maybeComplete()
          }
        }
      )

      setHandler(
        retry,
        new InHandler {
          override def onPush(): Unit = {
            addToBuffer(grab(retry))
            if (isAvailable(out)) push(out, buildToPush())
            else dropOrBackpressure()
            // Unlike the `in` handler, we always pull `retry` for the next
            // element to not block the put records flow
            pull(retry)
          }
        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            if (!aggregator.isEmpty || !buffer.isEmpty) push(out, buildToPush())
            else maybeComplete()
            // In case this shard buffered more data than average and the buffers
            // are overflown, we drop elements here to release space faster.
            if (!dropOrBackpressure() && !isClosed(in) && !hasBeenPulled(in)) pull(in)
            // Again, `retry` never backpressure to avoid blocking the records put flow.
            if (!isClosed(retry) && !hasBeenPulled(retry)) {
              println("PULL RETRY")
              pull(retry)
            }
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            super.onDownstreamFinish(cause)
            while (!buffer.isEmpty) onDrop(takeFromBuffer())
            memoryLimit.sync(metrics)
          }
        }
      )
    }
}
