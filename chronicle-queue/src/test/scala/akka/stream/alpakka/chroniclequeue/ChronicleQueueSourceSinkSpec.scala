/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.chroniclequeue

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.ByteString
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.reflect._

import akka.stream.alpakka.chroniclequeue.scaladsl._
import Timeouts._

abstract class ChronicleQueueSourceSinkSpec[T: ClassTag, Q <: impl.ChronicleQueueSerializer[T]: Manifest](
    typeName: String
) extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with Eventually {

  implicit val system = ActorSystem(s"Persistent${typeName}SourceSinkSpec", ChronicleQueueSpec.testConfig)
  implicit val mat = ActorMaterializer()
  implicit val serializer = ChronicleQueueSerializer[T]()
  implicit override val patienceConfig = PatienceConfig(timeout = Span(3, Seconds)) // extend eventually timeout for CI
  import SourceSinkSpecUtil._

  val transform = Flow[Int].map(createElement)

  override def afterAll =
    Await.ready(system.terminate(), awaitMax)

  def createElement(n: Int): T

  def format(element: T): String

  it should s"source and sink a stream of $elementCount elements" in {
    val util = new SourceSinkSpecUtil[T, T]
    import util._
    //#create-sink
    val sinkCQ = ChronicleQueue.sink[T](config)
    //#create-sink
    //#create-source
    val sourceCQ = ChronicleQueue.source[T](config, "source.idx")
    //#create-source

    val countFuture = sourceCQ.take(elementCount).runWith(flowCounter)

    in.via(transform).runWith(sinkCQ)

    val count = Await.result(countFuture, awaitMax)
    count shouldBe elementCount
    clean()
  }

  it should s"source and sink a stream of $elementCount elements using GraphDSL and custom config" in {
    val util = new SourceSinkSpecUtil[T, T]
    import util._
    val sinkCQ = ChronicleQueue.sink[T](config)
    val sourceCQ = ChronicleQueue.source[T](config, "source.idx")

    val streamGraph1 = RunnableGraph.fromGraph(GraphDSL.create(sinkCQ) { implicit builder => sink =>
      import GraphDSL.Implicits._
      in ~> transform ~> sink
      ClosedShape
    })

    val streamGraph2 = RunnableGraph.fromGraph(GraphDSL.create(flowCounter) { implicit builder => sink =>
      import GraphDSL.Implicits._
      val take = Flow[T].take(elementCount)

      sourceCQ ~> take ~> sink
      ClosedShape
    })

    val countFuture = streamGraph2.run()
    streamGraph1.run()

    val count = Await.result(countFuture, awaitMax)
    count shouldBe elementCount
    clean()
  }

}

class ChronicleByteStringSourceSinkSpec
    extends ChronicleQueueSourceSinkSpec[ByteString, ByteStringSerializer]("ByteString") {

  def createElement(n: Int): ByteString = ByteString(s"Hello $n")

  def format(element: ByteString): String = element.utf8String
}

class ChronicleStringSourceSinkSpec extends ChronicleQueueSourceSinkSpec[String, ObjectSerializer[String]]("Object") {

  def createElement(n: Int): String = s"Hello $n"

  def format(element: String): String = element
}

class ChronicleLongSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Long, LongSerializer]("Long") {

  def createElement(n: Int): Long = n

  def format(element: Long): String = element.toString
}

class ChronicleIntSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Int, IntSerializer]("Int") {

  def createElement(n: Int): Int = n

  def format(element: Int): String = element.toString
}

class ChronicleShortSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Short, ShortSerializer]("Short") {

  def createElement(n: Int): Short = n.toShort

  def format(element: Short): String = element.toString
}

class ChronicleByteSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Byte, ByteSerializer]("Byte") {

  def createElement(n: Int): Byte = n.toByte

  def format(element: Byte): String = element.toString
}

class ChronicleCharSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Char, CharSerializer]("Char") {

  def createElement(n: Int): Char = n.toChar

  def format(element: Char): String = element.toString
}

class ChronicleDoubleSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Double, DoubleSerializer]("Double") {

  def createElement(n: Int): Double = n.toDouble

  def format(element: Double): String = element.toString
}

class ChronicleFloatSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Float, FloatSerializer]("Float") {

  def createElement(n: Int): Float = n.toFloat

  def format(element: Float): String = element.toString
}

class ChronicleBooleanSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Boolean, BooleanSerializer]("Boolean") {

  def createElement(n: Int): Boolean = n % 2 == 0

  def format(element: Boolean): String = element.toString
}

class ChroniclePersonSourceSinkSpec extends ChronicleQueueSourceSinkSpec[Person, PersonSerializer]("Person") {

  override implicit val serializer = new PersonSerializer()

  def createElement(n: Int): Person = Person(s"John Doe $n", 20)

  def format(element: Person): String = element.toString
}
