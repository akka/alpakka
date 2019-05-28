/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.eip.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.{CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.{ActorMaterializer, FlowShape, Graph}
import akka.stream.scaladsl._
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class PassThroughExamples extends WordSpec with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val system = ActorSystem("Test")
  implicit val mat = ActorMaterializer()

  "PassThroughFlow" should {
    " original message is maintained " in {

      //#PassThroughWithKeep
      //Sample Source
      val source = Source(List(1, 2, 3))

      // Pass through this flow maintaining the original message
      val passThroughMe =
        Flow[Int]
          .map(_ * 10)

      val ret = source
        .via(PassThroughFlow(passThroughMe, Keep.right))
        .runWith(Sink.seq)

      //Verify results
      ret.futureValue should be(Vector(1, 2, 3))
      //#PassThroughWithKeep

    }

    " original message and pass through flow output are returned " in {

      //#PassThroughTuple
      //Sample Source
      val source = Source(List(1, 2, 3))

      // Pass through this flow maintaining the original message
      val passThroughMe =
        Flow[Int]
          .map(_ * 10)

      val ret = source
        .via(PassThroughFlow(passThroughMe))
        .runWith(Sink.seq)

      //Verify results
      ret.futureValue should be(Vector((10, 1), (20, 2), (30, 3)))
      //#PassThroughTuple

    }
  }

  override protected def afterAll(): Unit = system.terminate()
}

//#PassThrough
object PassThroughFlow {
  def apply[A, T](processingFlow: Flow[A, T, NotUsed]): Graph[FlowShape[A, (T, A)], NotUsed] =
    apply[A, T, (T, A)](processingFlow, Keep.both)

  def apply[A, T, O](processingFlow: Flow[A, T, NotUsed], output: (T, A) => O): Graph[FlowShape[A, O], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      {
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[A](2))
        val zip = builder.add(ZipWith[T, A, O]((left, right) => output(left, right)))

        // format: off
        broadcast.out(0) ~> processingFlow ~> zip.in0
        broadcast.out(1)         ~>           zip.in1
        // format: on

        FlowShape(broadcast.in, zip.out)
      }
    })
}
//#PassThrough

object PassThroughFlowKafkaCommitExample {
  implicit val system = ActorSystem("Test")
  implicit val mat = ActorMaterializer()

  def dummy(): Unit = {
    // #passThroughKafkaFlow
    val writeFlow = Flow[ConsumerMessage.CommittableMessage[String, Array[Byte]]].map(_ => ???)

    val consumerSettings =
      ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
    val committerSettings = CommitterSettings(system)

    Consumer
      .committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .via(PassThroughFlow(writeFlow, Keep.right))
      .map(_.committableOffset)
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
    // #passThroughKafkaFlow
  }
}
