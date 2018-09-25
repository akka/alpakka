/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.eip.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.{ActorMaterializer, FlowShape, Graph}
import akka.stream.scaladsl._
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._

class PassThroughExamples extends WordSpec with BeforeAndAfterAll with Matchers with ScalaFutures {

  implicit val system = ActorSystem("Test")
  implicit val mat = ActorMaterializer()

  "PassThroughFlow" should {
    " original message is maintained " in {

      //#PassThroughSimple
      //Sample Source
      val source = Source(List(1, 2, 3))

      // Pass through this flow maintained the original message
      val passThroughMe =
        Flow[Int]
          .map(_ * 10)

      val ret = source
        .via(PassThroughFlow(passThroughMe, Keep.right))
        .runWith(Sink.seq)

      //Verify results

      ret.futureValue should be(Vector(1, 2, 3))
      //#PassThroughSimple

    }

    " original message and pass through flow output are returned " in {

      //#PassThroughTuple
      //Sample Source
      val source = Source(List(1, 2, 3))

      // Pass through this flow maintained the original message
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

        Merge

        val tuplerizer = builder.add(Flow[A].map(x => (x, x)))
        val detuplerizer = builder.add(Flow[(T, A)].map(x => output(x._1, x._2)))

        val unzip = builder.add(Unzip[A, A]())
        val zip = builder.add(Zip[T, A]())

        val passThroughFlow = Flow[A].map(identity)

        tuplerizer ~> unzip.in
        unzip.out0 ~> processingFlow ~> zip.in0
        unzip.out1 ~> passThroughFlow ~> zip.in1
        zip.out ~> detuplerizer

        FlowShape(tuplerizer.in, detuplerizer.out)
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

      Consumer
        .committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .via(PassThroughFlow(writeFlow, Keep.right))
        .map(_.committableOffset)
        .groupedWithin(10, 5.seconds)
        .map(CommittableOffsetBatch(_))
        .mapAsync(3)(_.commitScaladsl())
        .toMat(Sink.ignore)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
    // #passThroughKafkaFlow
  }
}
