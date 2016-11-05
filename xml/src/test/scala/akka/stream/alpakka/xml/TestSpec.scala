/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.xml

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ ActorMaterializer, Materializer }
import akka.util.Timeout
import org.scalatest._
import org.scalatest.concurrent.{ Eventually, ScalaFutures }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import scala.xml.pull.XMLEvent

final case class Address(street: String = "", houseNumber: String = "", zipCode: String = "", city: String = "")

final case class Person(firstName: String = "", lastName: String = "", age: Int = 0, address: Address = Address())

trait TestSpec extends FlatSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with OptionValues
  with Eventually
  with ClasspathResources {

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val pc: PatienceConfig = PatienceConfig(timeout = 60.seconds)
  implicit val timeout = Timeout(30.seconds)

  val testPerson1 = Person("Barack", "Obama", 54, Address("Pennsylvania Ave", "1600", "20500", "Washington"))
  val testPerson2 = Person("Anon", "Ymous", 42, Address("Here", "1337", "12345", "InUrBase"))

  final val InvalidPerson = "xml/invalid-person.xml"
  final val PersonXmlFile = "xml/person.xml"
  final val PeopleXmlFile = "xml/people.xml"
  final val PeopleXsdFile = "xml/people.xsd"

  implicit class PimpedByteArray(self: Array[Byte]) {
    def getString: String = new String(self)
  }

  implicit class PimpedFuture[T](self: Future[T]) {
    def toTry: Try[T] = Try(self.futureValue)
  }

  def withTestXMLEventSource()(filename: String)(f: TestSubscriber.Probe[XMLEvent] => Unit): Unit =
    withInputStream(filename) { is =>
      f(XmlEventSource.fromInputStream(is).runWith(TestSink.probe[XMLEvent]))
    }

  def withTestXMLPersonParser()(filename: String)(f: TestSubscriber.Probe[Person] => Unit): Unit =
    withInputStream(filename) { is =>
      f(XmlEventSource.fromInputStream(is).via(PersonParser.flow).runWith(TestSink.probe[Person]))
    }

  implicit class SourceOps[A](src: Source[A, NotUsed]) {
    def testProbe(f: TestSubscriber.Probe[A] => Unit): Unit =
      f(src.runWith(TestSink.probe(system)))
  }

  def randomId = UUID.randomUUID.toString

  override protected def afterAll(): Unit = {
    system.terminate().toTry should be a 'success
  }
}
