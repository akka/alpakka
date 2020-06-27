/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.NotUsed
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.Attributes
import akka.stream.alpakka.googlecloud.bigquery.impl.util.DelayFlow.DelayStrategy
import akka.stream.scaladsl.Flow
import akka.stream.stage._

import scala.concurrent.duration._

private[util] object DelayFlow {

  /**
   * Flow with fixed delay for each element.
   * @param fixedDelay value of the delay
   */
  def apply[T](fixedDelay: FiniteDuration): Flow[T, T, NotUsed] =
    if (fixedDelay <= Duration.Zero)
      Flow[T]
    else
      DelayFlow[T](() => DelayStrategy.fixedDelay(fixedDelay))

  /**
   * Flow for universal delay management, allows to manage delay through [[DelayStrategy]].
   * It determines delay for each ongoing element invoking `DelayStrategy.nextDelay(elem: T): FiniteDuration`.
   * Implementing [[DelayStrategy]] with your own gives you flexible ability to manage delay value depending on coming elements.
   * It is important notice that [[DelayStrategy]] can be stateful.
   * There are also predefined strategies, see [[DelayStrategy]] companion object's methods.
   *
   *
   * For example:
   * {{{
   * //delay, infinitely increasing by `1 second` on every Failure
   *  new DelayStrategy[Try[AnyRef]]{
   *    var delay = Duration.Zero
   *    override def nextDelay(elem: Try[AnyRef]): FiniteDuration = {
   *      if(elem.isFailure){
   *        delay += (1 second)
   *      }
   *      delay
   *    }
   *  }
   * }}}
   * @param strategySupplier creates new [[DelayStrategy]] object for each materialization
   * @see [[DelayStrategy]]
   */
  def apply[T](strategySupplier: () => DelayStrategy[_ >: T]): Flow[T, T, NotUsed] =
    Flow.fromGraph(new DelayFlow[T](strategySupplier))

  object DelayStrategy {

    /**
     * Fixed delay strategy, always returns constant delay for any element.
     * @param delay value of the delay
     */
    def fixedDelay(delay: FiniteDuration): DelayStrategy[Any] = new DelayStrategy[Any] {
      override def nextDelay(elem: Any): FiniteDuration = delay
    }

    /**
     * Strategy with linear increasing delay.
     * It starts with `initialDelay` for each element,
     * increases by `increaseStep` every time when `needsIncrease` returns `true` up to `maxDelay`,
     * when `needsIncrease` returns `false` it resets to `initialDelay`.
     * @param increaseStep step by which delay is increased
     * @param needsIncrease if `true` delay increases, if `false` delay resets to `initialDelay`
     * @param initialDelay initial delay for each of elements
     * @param maxDelay limits maximum delay
     */
    def linearIncreasingDelay[T](increaseStep: FiniteDuration,
                                 needsIncrease: T => Boolean,
                                 initialDelay: FiniteDuration = Duration.Zero,
                                 maxDelay: Duration = Duration.Inf): DelayStrategy[T] = {
      require(increaseStep > Duration.Zero, "Increase step must be positive")
      require(maxDelay > initialDelay, "Max delay must be bigger than initial delay")

      new DelayStrategy[T] {

        private var delay = initialDelay

        override def nextDelay(elem: T): FiniteDuration = {
          if (needsIncrease(elem)) {
            val next = delay + increaseStep
            if (next < maxDelay) {
              delay = next
            } else {
              delay = maxDelay.asInstanceOf[FiniteDuration]
            }

          } else {
            delay = initialDelay
          }
          delay
        }

      }

    }

  }

  /**
   * Allows to manage delay and can be stateful to compute delay for any sequence of elements,
   * all elements go through nextDelay() updating state and returning delay for each element
   */
  trait DelayStrategy[T] {

    /**
     * Returns delay for ongoing element, `Duration.Zero` means passing without delay
     */
    def nextDelay(elem: T): FiniteDuration

  }

}

/**
 * Flow stage for universal delay management, allows to manage delay through [[DelayStrategy]].
 * It determines delay for each ongoing element invoking `DelayStrategy.nextDelay(elem: T): FiniteDuration`.
 * Implementing [[DelayStrategy]] with your own gives you flexible ability to manage delay value depending on coming elements.
 * It is important notice that [[DelayStrategy]] can be stateful.
 * There are also predefined strategies, see [[DelayStrategy]] companion object's methods.
 * @param strategySupplier creates new [[DelayStrategy]] object for each materialization
 * @see [[DelayStrategy]]
 */
private[util] final class DelayFlow[T](strategySupplier: () => DelayStrategy[_ >: T])
    extends SimpleLinearGraphStage[T] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private case object DelayTimerKey

      private val strategy = strategySupplier()

      private var delayedElem: AnyRef = _

      override def onPush(): Unit = {
        val elem = grab(in)
        val delay = strategy.nextDelay(elem)
        if (delay <= Duration.Zero) {
          push(out, elem)
        } else {
          delayedElem = elem.asInstanceOf[AnyRef]
          scheduleOnce(DelayTimerKey, delay)
        }
      }

      override def onPull(): Unit =
        pull(in)

      override def onTimer(timerKey: Any): Unit = {
        push(out, delayedElem.asInstanceOf[T])
        delayedElem = null
        if (isClosed(in)) {
          completeStage()
        }
      }

      override def onUpstreamFinish(): Unit =
        if (!isTimerActive(DelayTimerKey)) {
          completeStage()
        }

      setHandler(out, this)
      setHandler(in, this)
    }

}
