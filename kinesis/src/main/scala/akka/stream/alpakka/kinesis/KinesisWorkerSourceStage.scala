/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util
import java.util.concurrent.Semaphore

import akka.stream.alpakka.kinesis.KinesisErrors.WorkerUnexpectedShutdown
import akka.stream.alpakka.kinesis.worker.{CommittableRecord, IRecordProcessor}
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class KinesisWorkerSourceStage(
    bufferSize: Int,
    checkWorkerPeriodicity: FiniteDuration,
    workerBuilder: IRecordProcessorFactory => Worker
)(implicit executor: ExecutionContext)
    extends GraphStage[SourceShape[CommittableRecord]] {

  private val out: Outlet[CommittableRecord] = Outlet("KinesisWorkerSourceStage.out")
  override val shape: SourceShape[CommittableRecord] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with StageLogging {
      private val buffer = new util.ArrayDeque[CommittableRecord]()
      // We're transmitting backpressure to the worker with a semaphore instance
      // semaphore.acquire ~> callback ~> push downstream ~> semaphore.release
      private var semaphore: Semaphore = _
      private var worker: Worker = _

      private def tryToProduce(): Unit =
        if (!buffer.isEmpty && isAvailable(out)) {
          push(out, buffer.poll())
          semaphore.release()
        }

      override protected def onTimer(timerKey: Any): Unit =
        if (worker.hasGracefulShutdownStarted && isAvailable(out)) {
          failStage(WorkerUnexpectedShutdown)
        }

      override def preStart(): Unit = {
        semaphore = new Semaphore(bufferSize)

        val callback = getAsyncCallback[CommittableRecord] { record =>
          buffer.offer(record)
          tryToProduce()
        }
        worker = workerBuilder(
          new IRecordProcessorFactory {
            override def createProcessor(): IRecordProcessor =
              new IRecordProcessor(record => {
                semaphore.acquire()
                callback.invoke(record)
              })
          }
        )
        log.info(s"Created Worker instance {} of application {}", worker, worker.getApplicationName)
        schedulePeriodically("check-worker-shutdown", checkWorkerPeriodicity)
        executor.execute(worker)
      }

      override def postStop(): Unit = {
        buffer.clear()
        Future(worker.shutdown())
        log.info(s"Shut down Worker instance {} of application {}", worker, worker.getApplicationName)
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          tryToProduce()
      })
    }

}
