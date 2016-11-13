/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.druid.internal

import java.util.concurrent.atomic.AtomicInteger

import akka.stream.alpakka.druid.TranquilizerSettings
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage._
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.tranquilizer.{MessageDroppedException, Tranquilizer}
import com.twitter.util.{Return, Throw}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class TranquilizerStage[A](settings: TranquilizerSettings[A]) extends GraphStage[FlowShape[A, Future[A]]] {

  private val in = Inlet[A]("messages")
  private val out = Outlet[Future[A]]("result")

  override val shape = FlowShape(in, out)

  @scala.throws[Exception](classOf[Exception])
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    val druidSender: Tranquilizer[A] = DruidBeams
      .fromConfig(settings.dataSourceConfig, settings.timestamper, settings.objectWriter)
      .partitioner(settings.partitioner)
      .buildTranquilizer(settings.dataSourceConfig.tranquilizerBuilder())

    new GraphStageLogic(shape) with StageLogging with InHandler with OutHandler {

      val awaitingConfirmation = new AtomicInteger(0)
      @volatile var inIsClosed = false
      var completionState: Option[Try[Unit]] = None

      override def preStart(): Unit = {
        super.preStart()

        druidSender.start()

      }

      setHandler(in, this)
      setHandler(out, this)

      private def checkForCompletion() =
        if (isClosed(in) && awaitingConfirmation.get == 0) {
          druidSender.stop()
          completionState match {
            case Some(Success(_)) => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }

      private val checkForCompletionCB = getAsyncCallback[Unit] { _ =>
        checkForCompletion()
      }

      override def onPush(): Unit = {
        val msg = grab(in)
        val r = Promise[A]

        druidSender.send(msg) respond {
          case Return(_) =>
            if (log.isDebugEnabled)
              log.debug(s"Sent message: $msg")
            r.success(msg)
          case Throw(e: MessageDroppedException) =>
            if (log.isDebugEnabled)
              log.debug(s"Dropped message: $msg", e)
            r.failure(e)
          case Throw(e) =>
            log.error("Failed to send message: $values", e)
            r.failure(e)
        } ensure {
          if (awaitingConfirmation.decrementAndGet() == 0 && inIsClosed)
            checkForCompletionCB.invoke(())
        }

        awaitingConfirmation.incrementAndGet()
        push(out, r.future)
      }

      override def onUpstreamFinish() = {
        log.info(s"Upstream finished!")
        inIsClosed = true
        completionState = Some(Success(()))
      }

      override def onUpstreamFailure(ex: Throwable) = {
        val dataSource = settings.dataSourceConfig.dataSource
        log.error(ex, s"Upstream failed on dataSoource: $dataSource!")
        completionState = Some(Failure(ex))
        fail(out, ex)
      }

      override def onPull(): Unit = tryPull(in)

      override def postStop(): Unit = {
        inIsClosed = true
        checkForCompletion()
      }
    }

  }
}
