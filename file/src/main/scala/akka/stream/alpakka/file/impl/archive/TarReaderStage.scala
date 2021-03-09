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

      readHeader(ByteString.empty)

      def readHeader(buffer: ByteString): Unit = {
        if (buffer.length >= TarArchiveEntry.headerLength) {
          readFile(buffer)
        } else setHandlers(flowIn, flowOut, new CollectHeader(buffer))
      }

      def readFile(headerBuffer: ByteString): Unit = {
        def pushSource(metadata: TarArchiveMetadata, buffer: ByteString): Unit = {
          if (buffer.length >= metadata.size) {
            val (emit, remain) = buffer.splitAt(metadata.size.toInt)
            log.debug(s"emitting completed source for $metadata")
            push(flowOut, metadata -> Source.single(emit))
            readTrailer(metadata, remain, subSource = None)
          } else setHandlers(flowIn, flowOut, new CollectFile(metadata, buffer))
        }

        if (headerBuffer.head == 0) {
          log.debug("empty filename, detected EOF padding, completing")
          complete(flowOut)
          setHandlers(flowIn, flowOut, new FlushEndOfFilePadding())
        } else {
          val metadata = TarArchiveEntry.parse(headerBuffer)
          val buffer = headerBuffer.drop(TarArchiveEntry.headerLength)
          if (isAvailable(flowOut)) {
            pushSource(metadata, buffer)
          } else {
            setHandlers(flowIn, flowOut, new PushSourceOnPull(metadata, buffer))
          }
        }

        final class PushSourceOnPull(metadata: TarArchiveMetadata, buffer: ByteString)
            extends OutHandler
            with InHandler {
          override def onPull(): Unit = {
            setHandler(flowOut, IgnoreDownstreamPull)
            pushSource(metadata, buffer)
          }

          // fail on upstream push
          override def onPush(): Unit = failStage(new TarReaderException("upstream pushed unexpectedly"))
          override def onUpstreamFinish(): Unit = setKeepGoing(true)
        }
      }

      def readTrailer(metadata: TarArchiveMetadata,
                      buffer: ByteString,
                      subSource: Option[SubSourceOutlet[ByteString]]): Unit = {
        val trailerLength = TarArchiveEntry.trailerLength(metadata)
        if (buffer.length >= trailerLength) {
          subSource.foreach(_.complete())
          if (isClosed(flowIn)) completeStage()
          readHeader(buffer.drop(trailerLength))
        } else setHandlers(flowIn, flowOut, new ReadPastTrailer(metadata, buffer, subSource))
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
       * Don't react on downstream pulls until we have something to push.
       */
      private trait IgnoreDownstreamPull extends OutHandler {
        final override def onPull(): Unit = ()
      }
      private object IgnoreDownstreamPull extends IgnoreDownstreamPull

      /**
       * Pull upstream on a downstream pull and ignore subsequent pulls.
       */
      private trait ExpectDownstreamPull extends OutHandler {
        final override def onPull(): Unit = {
          pull(flowIn)
          setHandler(flowOut, IgnoreDownstreamPull)
        }
      }
      private object ExpectDownstreamPull extends ExpectDownstreamPull

      /**
       * Handler until the header of 512 bytes is completely received.
       */
      private final class CollectHeader(var buffer: ByteString) extends InHandler with ExpectDownstreamPull {

        override def onPush(): Unit = {
          buffer ++= grab(flowIn)
          if (buffer.length >= TarArchiveEntry.headerLength) {
            readFile(buffer)
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
      private final class CollectFile(metadata: TarArchiveMetadata, var buffer: ByteString)
          extends InHandler
          with IgnoreDownstreamPull {
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
        setHandler(flowOut, IgnoreDownstreamPull)

        private def subPush(bs: ByteString): Unit = {
          val remaining = metadata.size - emitted
          if (remaining <= bs.length) {
            val (emit, remain) = bs.splitAt(remaining.toInt)
            subSource.push(emit)
            readTrailer(metadata, remain, Some(subSource))
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
          extends InHandler
          with ExpectDownstreamPull {
        private val trailerLength = TarArchiveEntry.trailerLength(metadata)

        override def onPush(): Unit = {
          // TODO the buffer content doesn't need to be kept
          buffer ++= grab(flowIn)
          if (buffer.length >= trailerLength) {
            subSource.foreach { src =>
              src.complete()
              setHandler(flowOut, ExpectDownstreamPull)
              if (isAvailable(flowOut)) pull(flowIn)
            }
            readHeader(buffer.drop(trailerLength))
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
      private final class FlushEndOfFilePadding() extends InHandler with IgnoreDownstreamPull {

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
