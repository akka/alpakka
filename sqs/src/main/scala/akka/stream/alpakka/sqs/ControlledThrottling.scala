/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.time.Clock

import akka.stream.Attributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}

import scala.concurrent.duration.FiniteDuration

class ControlledThrottling[T](clock: Clock, delay: FiniteDuration) extends SimpleLinearGraphStage[T] {
  private var isThrottling: Boolean = false

  def setThrottling(shouldBeThrottled: Boolean): Unit =
    isThrottling = shouldBeThrottled

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private case object DelayTimerKey

      private var lastPull: Long = 0

      def onPush(): Unit = {
        val elem = grab(in)

        push(out, elem)
      }

      def onPull(): Unit = delayfulPull()

      override def onTimer(timerKey: Any): Unit = timerKey match {
        case DelayTimerKey => delayfulPull()
      }

      private def delayfulPull(): Unit =
        if (isThrottling) {
          if (clock.millis() - lastPull < delay.toMillis) {
            scheduleOnce(DelayTimerKey, delay)
          } else {
            lastPull = clock.millis()
            pull(in)
          }
        } else {
          lastPull = clock.millis()
          pull(in)
        }

      setHandler(out, this)
      setHandler(in, this)
    }
}
