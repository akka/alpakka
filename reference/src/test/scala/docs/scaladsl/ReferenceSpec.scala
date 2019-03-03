/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.reference.scaladsl.Reference
import akka.stream.alpakka.reference._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.immutable
import scala.concurrent.Future

/**
 * Append "Spec" to every Scala test suite.
 */
class ReferenceSpec extends WordSpec with BeforeAndAfterAll with ScalaFutures with Matchers {

  implicit val sys = ActorSystem("ReferenceSpec")
  implicit val mat: Materializer = ActorMaterializer()

  final val ClientId = "test-client-id"

  "reference connector" should {

    /**
     * Type annotations not generally needed on local variables.
     * However it allows to check if the types are really what we want.
     */
    "compile settings" in {
      val providedAuth: Authentication.Provided =
        Authentication.Provided().withVerifier(c => true)

      val noAuth: Authentication.None =
        Authentication.None

      val settings: SourceSettings = SourceSettings(ClientId)

      settings.withAuthentication(providedAuth)
      settings.withAuthentication(noAuth)
    }

    "compile source" in {
      // #source
      val settings: SourceSettings = SourceSettings(ClientId)

      val source: Source[ReferenceReadResult, Future[Done]] =
        Reference.source(settings)
      // #source
    }

    "compile flow" in {
      // #flow
      val flow: Flow[ReferenceWriteMessage, ReferenceWriteResult, NotUsed] =
        Reference.flow()
      // #flow

      implicit val ec = scala.concurrent.ExecutionContext.global
      val flow2: Flow[ReferenceWriteMessage, ReferenceWriteResult, NotUsed] =
        Reference.flow()
    }

    "run source" in {
      val source = Reference.source(SourceSettings(ClientId))

      val msg = source.runWith(Sink.head).futureValue
      msg.data should contain theSameElementsAs Seq(ByteString("one"))
    }

    "run flow" in {
      val flow = Reference.flow()
      val source = Source(
        immutable.Seq(
          ReferenceWriteMessage()
            .withData(immutable.Seq(ByteString("one")))
            .withMetrics(Map("rps" -> 20L, "rpm" -> 30L)),
          ReferenceWriteMessage().withData(
            immutable.Seq(
              ByteString("two"),
              ByteString("three"),
              ByteString("four")
            )
          ),
          ReferenceWriteMessage().withData(
            immutable.Seq(
              ByteString("five"),
              ByteString("six"),
              ByteString("seven")
            )
          )
        )
      )

      val result = source.via(flow).runWith(Sink.seq).futureValue

      result.flatMap(_.message.data) should contain theSameElementsAs Seq(
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven"
      ).map(ByteString.apply)

      result.head.metrics.get("total") should contain(50L)
    }

  }

  override def afterAll() =
    TestKit.shutdownActorSystem(sys)

}
