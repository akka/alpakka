/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb

import akka.Done
import akka.stream.stage._
import akka.stream.{Attributes, Inlet, SinkShape}
import org.kairosdb.client.Client
import org.kairosdb.client.builder.MetricBuilder
import org.kairosdb.client.response.Response

import scala.concurrent._
import scala.util.{Failure, Success}

final case class KairosSinkSettings(parallelism: Int) {
  require(parallelism > 0)

}

object KairosSinkSettings {
  val Defaults = KairosSinkSettings(1)
}

private object NullExecutionContext extends ExecutionContext {
  override def execute(runnable: Runnable): Unit = ???

  override def reportFailure(cause: Throwable): Unit = ???
}

class KairosDBSinkStage(settings: KairosSinkSettings,
                        kairosClient: Client)(implicit executionContext: ExecutionContext)
    extends GraphStageWithMaterializedValue[SinkShape[MetricBuilder], Future[Done]] {

  val in: Inlet[MetricBuilder] = Inlet("KairosSink.in")

  override val shape: SinkShape[MetricBuilder] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new KairosSinkStageLogic(in, shape, kairosClient, promise, settings)

    (logic, promise.future)
  }
}

private[kairosdb] class KairosSinkStageLogic(
    in: Inlet[MetricBuilder],
    shape: SinkShape[MetricBuilder],
    kairosClient: Client,
    promise: Promise[Done],
    settings: KairosSinkSettings
)(implicit executionContext: ExecutionContext)
    extends GraphStageLogic(shape)
    with StageLogging {

  private var runningPushes = 0
  private var isShutdownInProgress = false
  private var successCallback: AsyncCallback[Response] = _
  private var failureCallback: AsyncCallback[Throwable] = _

  setHandler(in, new InHandler {
    override def onPush() =
      triggerPushMetric()

    override def onUpstreamFinish(): Unit = {
      isShutdownInProgress = true
      tryShutdown()
    }
  })

  override def preStart(): Unit = {
    setKeepGoing(true)
    pull(in)
    successCallback = getAsyncCallback[Response](handleSuccess)
    failureCallback = getAsyncCallback[Throwable](handleFailure)
  }

  private def triggerPushMetric(): Unit = {
    runningPushes += 1
    val builder = grab(in)
    val task = Future {
      blocking {
        kairosClient.pushMetrics(builder)
      }
    }

    task.onComplete {
      case Success(response) => successCallback.invoke(response)
      case Failure(exception) => failureCallback.invoke(exception)
    }
  }

  private def handleSuccess(response: Response) = {
    runningPushes -= 1
    tryShutdown()
    tryPull()
  }

  private def tryShutdown(): Unit = if (isShutdownInProgress && runningPushes <= 0) {
    completeStage()
    promise.trySuccess(Done)
  }

  private def tryPull(): Unit = if (runningPushes < settings.parallelism) {
    pull(in)
  }

  private def handleFailure(t: Throwable): Unit = {
    log.error(t, "KairosDB Client failure: {}", t.getMessage)
    runningPushes -= 1
    failStage(t)
    promise.tryFailure(t)
  }
}
