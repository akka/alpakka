/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import java.nio.file.{OpenOption, Path, StandardOpenOption}

import akka.Done
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.stage._
import akka.util.ByteString

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object LogRotatorSink {
  def apply(
      functionGeneratorFunction: () => ByteString => Option[Path],
      fileOpenOptions: Set[OpenOption] = Set(StandardOpenOption.APPEND, StandardOpenOption.CREATE)
  ): Sink[ByteString, Future[Done]] =
    Sink.fromGraph(new LogRotatorSink(functionGeneratorFunction, fileOpenOptions))
}

final private[scaladsl] class LogRotatorSink(functionGeneratorFunction: () => ByteString => Option[Path],
                                             fileOpenOptions: Set[OpenOption])
    extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Done]] {

  val in = Inlet[ByteString]("FRotator.in")
  override val shape = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new GraphStageLogic(shape) {
      val pathGeneratorFunction: ByteString => Option[Path] = functionGeneratorFunction()
      var sourceOut: SubSourceOutlet[ByteString] = _
      var fileSinkCompleted: Option[Future[IOResult]] = Option.empty
      val decider =
        inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)

      def failThisStage(ex: Throwable): Unit =
        if (!promise.isCompleted) {
          if (sourceOut != null) {
            sourceOut.fail(ex)
          }
          cancel(in)
          promise.failure(ex)
        }

      def generatePathOrFailPeacefully(data: ByteString): Option[Path] = {
        var ret = Option.empty[Path]
        try {
          ret = pathGeneratorFunction(data)
        } catch {
          case ex: Throwable =>
            failThisStage(ex)
        }
        ret
      }

      def fileSinkFutureCallbackHandler(result: Try[IOResult], future: Future[IOResult]): Unit =
        result match {
          case Success(IOResult(_, Failure(ex))) if decider(ex) == Supervision.Stop =>
            promise.failure(ex)
          case Success(x) if fileSinkCompleted.isDefined && fileSinkCompleted.get == future =>
            promise.trySuccess(Done)
            completeStage()
          case Failure(ex) =>
            failThisStage(ex)
          case _ =>
        }

      //init stage where we are waiting for the first path
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val data = grab(in)
            val pathO = generatePathOrFailPeacefully(data)
            pathO.fold(
              if (!isClosed(in)) pull(in)
            )(
              switchPath(_, data)
            )
          }

          override def onUpstreamFinish(): Unit =
            completeStage()

          override def onUpstreamFailure(ex: Throwable): Unit =
            failThisStage(ex)
        }
      )

      //we must pull the first element cos we are a sink
      override def preStart(): Unit = {
        super.preStart()
        pull(in)
      }

      //we recreate the tail of the stream, and emit the data for the next req
      def switchPath(path: Path, data: ByteString): Unit = {
        if (sourceOut ne null) {
          fileSinkCompleted = Option.empty
          sourceOut.complete()
        }
        sourceOut = new SubSourceOutlet[ByteString]("FRotatorSource")
        sourceOut.setHandler(new OutHandler {
          override def onPull(): Unit = {
            sourceOut.push(data)
            switchToNormalMode()
          }
        })
        fileSinkCompleted = Some(
          Source
            .fromGraph(sourceOut.source)
            .runWith(FileIO.toPath(path, fileOpenOptions))(interpreter.subFusingMaterializer)
        )
        fileSinkCompleted
          .foreach(
            future =>
              future.onComplete(res => fileSinkFutureCallbackHandler(res, future))(
                akka.dispatch.ExecutionContexts.sameThreadExecutionContext
            )
          )
      }

      //we change path if needed or push the grabbed data
      def switchToNormalMode(): Unit = {
        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val data = grab(in)
              val pathO = generatePathOrFailPeacefully(data)
              pathO.fold(
                sourceOut.push(data)
              )(
                switchPath(_, data)
              )
            }

            override def onUpstreamFinish(): Unit =
              sourceOut.complete()

            override def onUpstreamFailure(ex: Throwable): Unit =
              failThisStage(ex)
          }
        )
        sourceOut.setHandler(new OutHandler {
          override def onPull(): Unit =
            pull(in)
        })
      }
    }
    (logic, promise.future)
  }

}
