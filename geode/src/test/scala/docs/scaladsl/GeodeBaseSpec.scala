/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.{Date, UUID}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.geode.{GeodeSettings, RegionSettings}
import akka.stream.scaladsl.Source
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class GeodeBaseSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  implicit val system = ActorSystem("test")
  implicit val materializer = ActorMaterializer()

  //#region
  val personsRegionSettings: RegionSettings[Int, Person] = RegionSettings("persons", (p: Person) => p.id)
  val animalsRegionSettings: RegionSettings[Int, Animal] = RegionSettings("animals", (a: Animal) => a.id)
  val complexesRegionSettings: RegionSettings[UUID, Complex] = RegionSettings("complexes", (a: Complex) => a.id)

  //#region

  /**
   * Run IT test only if geode is available.
   * @param f
   */
  def it(f: GeodeSettings => Unit): Unit =
    f(GeodeSettings(sys.env.get("IT_GEODE_HOSTNAME").getOrElse("localhost")))

  protected def buildPersonsSource(range: Range): Source[Person, Any] =
    Source(range).map(i => Person(i, s"Person Scala $i", new Date()))

  protected def buildAnimalsSource(range: Range): Source[Animal, Any] =
    Source(range).map(i => Animal(i, s"Animal Scala $i", 1))

  protected def buildComplexesSource(range: Range): Source[Complex, Any] =
    Source(range).map(i => Complex(UUID.randomUUID(), List(1, 2, 3), List(new Date()), Set(UUID.randomUUID())))

  override protected def afterAll(): Unit =
    Await.result(system.terminate(), 10 seconds)
}
