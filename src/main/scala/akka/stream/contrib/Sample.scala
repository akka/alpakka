package akka.stream.contrib

import java.util.Random
import java.util.concurrent.ThreadLocalRandom

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}


object Sample {
  /**
    *
    * returns every nth elements
    *
    * @param nth must > 0
    * @tparam T
    * @return
    */
  def apply[T](nth: Int): Sample[T] = Sample[T](() => nth)

  /**
    *
    * randomly sampling on a stream
    *
    * @param maxStep must > 0, default 1000, the randomly step will be between 1 (inclusive) and maxStep (inclusive)
    * @tparam T
    * @return
    */
  def random[T](maxStep: Int = 1000): Sample[T] = {
    require(maxStep > 0, "max step for a random sampling must > 0")
    Sample[T](() => ThreadLocalRandom.current().nextInt(maxStep) + 1)
  }
}


/**
  * supports sampling on stream
  *
  * @param next a lambda returns next sample position
  * @tparam T
  */
case class Sample[T](next: () => Int) extends GraphStage[FlowShape[T, T]] {
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    var step = getNextStep()
    var counter = 0

    def onPull(): Unit = {
      pull(in)
    }

    def onPush(): Unit = {
      counter += 1
      if (counter >= step) {
        counter = 0
        step = getNextStep()
        push(out, grab(in))
      } else {
        pull(in)
      }
    }

    private def getNextStep(): Long = {
      val nextStep = next()
      require(nextStep > 0, s"sampling step should be a positive value: ${nextStep}")
      nextStep
    }

    setHandlers(in, out, this)
  }

  val in = Inlet[T]("Sample-in")
  val out = Outlet[T]("Sample-out")
  override val shape = FlowShape(in, out)
}
