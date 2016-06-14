package akka.stream.contrib

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

object Sample
{
  /**
    *
    * returns every nth elements
    *
    * @param nth
    * @tparam T
    * @return
    */
  def apply[T](nth: Int): Sample[T] = Sample[T](() => nth)

  // def random[T](maxStep: Int = 1000, random: Random = Random): Sample[T] = Sample[T](() => random.nextInt(maxStep))
}


/**
  * supports sampling on stream
  *
  * @param next a lambda returns next sample position
  * @tparam T
  */
case class Sample[T](next: () => Int) extends GraphStage[FlowShape[T, T]]
{
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    var step    = next()
    var counter = 0

    @throws[Exception](classOf[Exception])
    override def onPull(): Unit = {
      if (!hasBeenPulled(in)) {
        pull(in)
      }
    }

    @throws[Exception](classOf[Exception])
    override def onUpstreamFinish(): Unit = {
      completeStage()
    }

    @throws[Exception](classOf[Exception])
    override def onPush(): Unit = {
      counter += 1
      if (counter == step) {
        counter = 0
        step = next()
        push(out, grab(in))
      } else {
        pull(in)
      }
    }

    setHandlers(in, out, this)
  }

  private  val in    = Inlet[T]("Sample-in")
  private  val out   = Outlet[T]("Sample-out")
  override val shape = FlowShape(in, out)
}
