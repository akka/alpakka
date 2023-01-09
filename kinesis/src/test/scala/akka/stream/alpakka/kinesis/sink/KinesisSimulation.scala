package akka.stream.alpakka.kinesis.sink

import akka.NotUsed
import akka.stream._
import akka.stream.alpakka.kinesis.KinesisLimits._
import akka.stream.alpakka.kinesis.sink.KinesisSink.{Result, Retryable, Succeeded}
import akka.stream.scaladsl.{Flow, GraphDSL, Partition, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable
import scala.concurrent.ExecutionContext.parasitic
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}

class SplitPreferredShape[T](val in: Inlet[T], val preferred: Outlet[T], val backup: Outlet[T]) extends Shape {
  override def inlets: immutable.Seq[Inlet[_]] = immutable.Seq(in)
  override def outlets: immutable.Seq[Outlet[_]] = immutable.Seq(preferred, backup)
  override def deepCopy(): Shape = new SplitPreferredShape(in.carbonCopy(), preferred.carbonCopy(), backup.carbonCopy())
}

class SplitPreferred[T] extends GraphStage[SplitPreferredShape[T]] {
  val in: Inlet[T] = Inlet("in")
  val preferred: Outlet[T] = Outlet("preferred")
  val backup: Outlet[T] = Outlet("backup")

  override val shape = new SplitPreferredShape(in, preferred, backup)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      setHandler(in, this)
      setHandler(preferred, this)
      setHandler(backup, this)

      override def onPush(): Unit = {
        if (isAvailable(preferred)) {
          push(preferred, grab(in))
          pull(in)
        } else if (isAvailable(backup)) {
          push(backup, grab(in))
          pull(in)
        }
      }

      override def onPull(): Unit = {
        if (isAvailable(in)) onPush()
        else if (!isClosed(in) && !hasBeenPulled(in)) pull(in)
      }
    }
}

object KinesisSimulation {
  type PutRecords = Iterable[Aggregated] => Future[Iterable[Result]]

  def putRecords(nrOfShards: Int)(implicit mat: Materializer): PutRecords = {
    val (queue, source) = Source
      .queue[(Iterable[Aggregated], Promise[Iterable[Result]])](
        0,
        OverflowStrategy.backpressure,
        maxConcurrentOffers = Int.MaxValue
      )
      .preMaterialize()

    source.runWith(GraphDSL.create() { implicit b: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      type Elem = (Aggregated, Promise[Result])

      def addDenyOnOverflow(): SinkShape[Elem] = {
        val denyOnOverflow = b.add(new SplitPreferred[Elem])

        val accept = b.add(
          Flow[Elem]
            .throttle(MaxRecordsPerSecondPerShard, 1.second)
            .throttle(MaxBytesPerSecondPerShard, 1.second, _._1.payloadBytes)
            .to(Sink.foreach { case (r, p) => p.trySuccess(Succeeded(r)) })
        )

        val deny = b.add(Sink.foreach[Elem] { case (r, p) => p.trySuccess(Retryable(r)) })

        denyOnOverflow.preferred ~> accept
        denyOnOverflow.backup ~> deny

        SinkShape(denyOnOverflow.in)
      }

      val requests = b.add(
        Flow[(Iterable[Aggregated], Promise[Iterable[Result]])]
          .delay(50.millis, DelayOverflowStrategy.backpressure)
          .mapConcat {
            case (entries, p) =>
              implicit val ec: ExecutionContext = parasitic
              val elems = entries.map((_, Promise[Result]()))
              p.completeWith(Future.sequence(elems.map(_._2.future)))
              elems
          }
      )

      val partition = b.add(Partition[Elem](nrOfShards, _._1.shardIndex))

      requests ~> partition
      for (i <- 0 until nrOfShards) {
        partition.out(i) ~> addDenyOnOverflow()
      }

      SinkShape(requests.in)
    })

    (request: Iterable[Aggregated]) => {
      val p = Promise[Iterable[Result]]()
      queue
        .offer((request, p))
        .andThen {
          case util.Failure(_) => p.success(request.map(Retryable(_)))
        }(parasitic)
      p.future
    }
  }
}
