/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.scaladsl

import java.nio.file.{OpenOption, Path, StandardOpenOption}

import akka.Done
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream._
import akka.stream.impl.fusing.MapAsync.{Holder, NotYetThere}
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.stage._
import akka.util.ByteString

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object LogRotatorSink {

  /**
   * Sink directing the incoming `ByteString`s to new files whenever `generatorFunction` returns a new value.
   *
   * @param functionGeneratorFunction creates a function that triggers rotation by returning a value
   * @param fileOpenOptions file options for file creation
   */
  def apply(
      functionGeneratorFunction: () => ByteString => Option[Path],
      fileOpenOptions: Set[OpenOption] = Set(StandardOpenOption.APPEND, StandardOpenOption.CREATE)
  ): Sink[ByteString, Future[Done]] =
    Sink.fromGraph(
      new LogRotatorSink[Path, IOResult](functionGeneratorFunction, sinkFactory = FileIO.toPath(_, fileOpenOptions))
    )

  /**
   * Sink directing the incoming `ByteString`s to a new `Sink` created bye `sinkFactory` whenever `generatorFunction returns a new value.
   *
   * @param functionGeneratorFunction creates a function that triggers rotation by returning a value
   * @param sinkFactory creates sinks for `ByteString`s from the value returned by `functionGeneratorFunction`
   * @tparam C criterion type (for files a `Path`)
   * @tparam R result type in materialized futures of `sinkFactory`
   **/
  def withSinkFactory[C, R](
      functionGeneratorFunction: () => ByteString => Option[C],
      sinkFactory: C => Sink[ByteString, Future[R]]
  ): Sink[ByteString, Future[Done]] =
    Sink.fromGraph(new LogRotatorSink[C, R](functionGeneratorFunction, sinkFactory))
}

/**
 * "Log Rotator Sink" graph stage
 *
 * @param functionGeneratorFunction creates a function that triggers rotation by returning a value
 * @param sinkFactory creates sinks for `ByteString`s from the value returned by `functionGeneratorFunction`
 * @tparam C criterion type (for files a `Path`)
 * @tparam R result type in materialized futures of `sinkFactory`
 */
final private class LogRotatorSink[C, R](functionGeneratorFunction: () => ByteString => Option[C],
                                         sinkFactory: C => Sink[ByteString, Future[R]])
    extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Done]] {

  val in = Inlet[ByteString]("FRotator.in")
  override val shape = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val decider =
      inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
    val logic = new Logic(promise, decider)
    (logic, promise.future)
  }

  private final class Logic(promise: Promise[Done], decider: Decider) extends GraphStageLogic(shape) {
    val pathGeneratorFunction: ByteString => Option[C] = functionGeneratorFunction()
    var sourceOut: SubSourceOutlet[ByteString] = _
    var fileSinkCompleted: immutable.Seq[Future[R]] = immutable.Seq.empty
    def failThisStage(ex: Throwable): Unit =
      if (!promise.isCompleted) {
        if (sourceOut != null) {
          sourceOut.fail(ex)
        }
        cancel(in)
        promise.failure(ex)
      }

    def generatePathOrFailPeacefully(data: ByteString): Option[C] = {
      var ret = Option.empty[C]
      try {
        ret = pathGeneratorFunction(data)
      } catch {
        case ex: Throwable =>
          failThisStage(ex)
      }
      ret
    }

    def fileSinkFutureCallbackHandler(future: Future[R])(h: Holder[R]): Unit =
      h.elem match {
        case Success(IOResult(_, Failure(ex))) if decider(ex) == Supervision.Stop =>
          promise.failure(ex)
        case Success(x) if fileSinkCompleted.size == 1 && fileSinkCompleted.head == future =>
          promise.trySuccess(Done)
          completeStage()
        case x: Success[R] =>
          fileSinkCompleted = fileSinkCompleted.filter(_ != future)
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

    def futureCB(newFuture: Future[R]) =
      getAsyncCallback[Holder[R]](fileSinkFutureCallbackHandler(newFuture))

    //we recreate the tail of the stream, and emit the data for the next req
    def switchPath(path: C, data: ByteString): Unit = {
      val prevOut = Option(sourceOut)

      sourceOut = new SubSourceOutlet[ByteString]("FRotatorSource")
      sourceOut.setHandler(new OutHandler {
        override def onPull(): Unit = {
          sourceOut.push(data)
          switchToNormalMode()
        }
      })
      val newFuture = Source
        .fromGraph(sourceOut.source)
        .runWith(sinkFactory(path))(interpreter.subFusingMaterializer)

      fileSinkCompleted = fileSinkCompleted :+ newFuture

      val holder = new Holder[R](NotYetThere, futureCB(newFuture))

      newFuture.onComplete(holder)(
        akka.dispatch.ExecutionContexts.sameThreadExecutionContext
      )

      prevOut.foreach(_.complete())
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

          override def onUpstreamFinish(): Unit = {
            implicit val executionContext: ExecutionContext =
              akka.dispatch.ExecutionContexts.sameThreadExecutionContext
            promise.completeWith(Future.sequence(fileSinkCompleted).map(_ => Done))
            sourceOut.complete()
          }

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

}
