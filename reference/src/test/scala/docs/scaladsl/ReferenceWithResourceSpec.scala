/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.reference.{ReferenceWriteMessage, ResourceExt}
import akka.stream.alpakka.reference.scaladsl.{ReferenceWithExternalResource, ReferenceWithResource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

/**
 * Append "Spec" to every Scala test suite.
 */
class ReferenceWithResourceSpec extends WordSpec with BeforeAndAfterAll with ScalaFutures with Matchers {

  implicit val sys = ActorSystem("ReferenceSpec")
  implicit val mat = ActorMaterializer()

  final val ClientId = "test-client-id"

  "reference with resource connector" should {

    "use global resource" in {
      val flow: Flow[ReferenceWriteMessage, ReferenceWriteMessage, NotUsed] =
        ReferenceWithResource.flow()

      Source
        .single(ReferenceWriteMessage())
        .via(flow)
        .to(Sink.seq)
        .run()
    }

    "use external resource" in {
      implicit val resource = ResourceExt().resource
      val flow = ReferenceWithExternalResource.flow()

      Source
        .single(ReferenceWriteMessage())
        .via(flow)
        .to(Sink.seq)
        .run()
    }
  }

  override def afterAll() =
    TestKit.shutdownActorSystem(sys)
}
