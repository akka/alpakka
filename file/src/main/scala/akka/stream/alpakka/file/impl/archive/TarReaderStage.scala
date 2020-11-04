/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl.archive

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.ActorAttributes.StreamSubscriptionTimeout
import akka.stream.alpakka.file.{TarArchiveMetadata, TarReaderException}
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream._
import akka.util.ByteString

import scala.concurrent.duration.FiniteDuration

/**
 * Internal API.
 */
@InternalApi
private[file] class TarReaderStage
    extends GraphStage[FlowShape[ByteString, (TarArchiveMetadata, Source[ByteString, NotUsed])]] {

  private val flowIn = Inlet[ByteString]("flowIn")
  private val flowOut = Outlet[(TarArchiveMetadata, Source[ByteString, NotUsed])]("flowOut")
  override val shape: FlowShape[ByteString, (TarArchiveMetadata, Source[ByteString, NotUsed])] =
    FlowShape(flowIn, flowOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging {
      import TarReaderStage._

      private final class FileOutSubSource
          extends SubSourceOutlet[ByteString]("fileOut")
          with TarReaderStage.SourceWithTimeout

      val ignoreFlowOutPull: OutHandler = new OutHandler {
        override def onPull(): Unit = ()
      }

      val expectFlowPull: OutHandler = new OutHandler {
        override def onPull(): Unit = {
          pull(flowIn)
          setHandler(flowOut, ignoreFlowOutPull)
        }
      }
      val failOnFlowPush: InHandler = new InHandler {
        override def onPush(): Unit = failStage(new TarReaderException("upstream pushed"))
        override def onUpstreamFinish(): Unit = setKeepGoing(true)
      }

      setHandler(flowIn, readHeader(ByteString.empty))
      setHandler(flowOut, expectFlowPull)

      def readHeader(buffer: ByteString): InHandler = {
        if (buffer.length >= TarArchiveEntry.headerLength) {
          readFile(buffer)
        } else new CollectHeader(buffer)
      }

      def readFile(headerBuffer: ByteString): InHandler = {
        def pushSource(metadata: TarArchiveMetadata, buffer: ByteString): InHandler = {
          if (buffer.length >= metadata.size) {
            val (emit, remain) = buffer.splitAt(metadata.size.toInt)
            log.debug(s"emitting completed source for $metadata")
            push(flowOut, metadata -> Source.single(emit))
            readTrailer(metadata, remain, subSource = None)
          } else new CollectFile(metadata, buffer)
        }

        if (headerBuffer.head == 0) {
          log.debug("empty filename, detected EOF padding, completing")
          complete(flowOut)
          new FlushEndOfFilePadding()
        } else {
          val metadata = TarArchiveEntry.parse(headerBuffer)
          val buffer = headerBuffer.drop(TarArchiveEntry.headerLength)
          if (isAvailable(flowOut)) {
            pushSource(metadata, buffer)
          } else {
            // await flow demand
            setHandler(flowOut, new OutHandler {
              override def onPull(): Unit = {
                setHandler(flowIn, pushSource(metadata, buffer))
              }
            })
            failOnFlowPush
          }
        }
      }

      def readTrailer(metadata: TarArchiveMetadata,
                      buffer: ByteString,
                      subSource: Option[SubSourceOutlet[ByteString]]): InHandler = {
        val trailerLength = TarArchiveEntry.trailerLength(metadata)
        if (buffer.length >= trailerLength) {
          subSource.foreach(_.complete())
          if (isClosed(flowIn)) completeStage()
          readHeader(buffer.drop(trailerLength))
        } else new ReadPastTrailer(metadata, buffer, subSource)
      }

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case SubscriptionTimeout(subSource) =>
            import StreamSubscriptionTimeoutTerminationMode._

            val timeoutSettings = attributes
              .get[ActorAttributes.StreamSubscriptionTimeout]
              .getOrElse(StreamSubscriptionTimeout(FiniteDuration(1, TimeUnit.MILLISECONDS), NoopTermination))
            val timeout = timeoutSettings.timeout

            timeoutSettings.mode match {
              case CancelTermination =>
                subSource.timeout(timeout)
                failStage(
                  new TarReaderException(
                    s"The tar content source was not subscribed to within $timeout, it must be subscribed to to progress tar file reading."
                  )
                )
              case WarnTermination =>
                log.warning(
                  "The tar content source was not subscribed to within {}, it must be subscribed to to progress tar file reading.",
                  timeout
                )
              case NoopTermination =>
            }
        }
      }

      /**
       * Handler until the header of 512 bytes is completely received.
       */
      private final class CollectHeader(var buffer: ByteString) extends InHandler {

        override def onPush(): Unit = {
          buffer ++= grab(flowIn)
          if (buffer.length >= TarArchiveEntry.headerLength) {
            setHandler(flowIn, readFile(buffer))
          } else pull(flowIn)
        }

        override def onUpstreamFinish(): Unit = {
          if (buffer.isEmpty) completeStage()
          else
            failStage(
              new TarReaderException(
                s"incomplete tar header: received ${buffer.length} bytes, expected ${TarArchiveEntry.headerLength} bytes"
              )
            )
        }
      }

      /**
       * Handler during file content reading.
       */
      private final class CollectFile(metadata: TarArchiveMetadata, var buffer: ByteString) extends InHandler {
        private var emitted: Long = 0
        private var flowInPulled = false

        private val subSource: FileOutSubSource = {
          val sub = new FileOutSubSource()
          val timeoutSignal = SubscriptionTimeout(sub)
          sub.setHandler(new OutHandler {
            override def onPull(): Unit = {
              cancelTimer(timeoutSignal)
              if (buffer.nonEmpty) {
                subPush(buffer)
                buffer = ByteString.empty
                if (isClosed(flowIn)) onUpstreamFinish()
              } else if (!flowInPulled) {
                flowInPulled = true
                pull(flowIn)
              }
            }
          })
          val timeout = attributes.get[ActorAttributes.StreamSubscriptionTimeout].get.timeout
          scheduleOnce(timeoutSignal, timeout)
          sub
        }

        log.debug(s"emitting source for $metadata")
        push(flowOut, metadata -> Source.fromGraph(subSource.source))
        setHandler(flowOut, ignoreFlowOutPull)

        def subPush(bs: ByteString) = {
          val remaining = metadata.size - emitted
          if (remaining <= bs.length) {
            subSource.push(bs.take(remaining.toInt))
            setHandler(flowIn, readTrailer(metadata, bs.drop(remaining.toInt), Some(subSource)))
          } else {
            subSource.push(bs)
            emitted += bs.length
          }
        }

        override def onPush(): Unit = {
          flowInPulled = false
          subPush(grab(flowIn))
        }

        override def onUpstreamFinish(): Unit = {
          if (buffer.isEmpty) {
            failStage(
              new TarReaderException(
                s"incomplete tar file contents for [${metadata.filePath}] expected ${metadata.size} bytes, received $emitted bytes"
              )
            )
          } else setKeepGoing(true)
        }

      }

      /**
       * Handler to read past the padding trailer.
       */
      private final class ReadPastTrailer(metadata: TarArchiveMetadata,
                                          var buffer: ByteString,
                                          subSource: Option[SubSourceOutlet[ByteString]])
          extends InHandler {
        val trailerLength = TarArchiveEntry.trailerLength(metadata)

        override def onPush(): Unit = {
          // TODO the buffer content doesn't need to be kept
          buffer ++= grab(flowIn)
          if (buffer.length >= trailerLength) {
            setHandler(flowIn, readHeader(buffer.drop(trailerLength)))
            subSource.foreach { src =>
              src.complete()
              setHandler(flowOut, expectFlowPull)
              if (isAvailable(flowOut)) pull(flowIn)
            }
          } else pull(flowIn)
        }

        override def onUpstreamFinish(): Unit = {
          if (buffer.length == trailerLength) completeStage()
          else
            failStage(
              new TarReaderException(
                s"incomplete tar file trailer for [${metadata.filePath}] expected ${trailerLength} bytes, received ${buffer.length} bytes"
              )
            )
        }
      }

      /**
       * "At the end of the archive file there are two 512-byte blocks filled with binary zeros as an end-of-file marker."
       */
      private final class FlushEndOfFilePadding() extends InHandler {

        override def onPush(): Unit = {
          grab(flowIn)
          pull(flowIn)
        }

        tryPull(flowIn)
      }

    }
}

object TarReaderStage {
  private trait SourceWithTimeout {
    def timeout(d: FiniteDuration): Unit
  }
  private final case class SubscriptionTimeout(subSource: TarReaderStage.SourceWithTimeout)
}
