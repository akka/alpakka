/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.pagetoken

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers

class EndOfStreamDetectorSpec
    extends TestKit(ActorSystem("PageTokenGeneratorSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  "EndOfStreamDetector" should {

    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val emptyPagingInfo = PagingInfo(None, None)
    val pagingInfoWithPageToken = PagingInfo(Some("next page"), None)

    "terminate processing if retry is false and page token is none" in {
      val sinkProbe = TestSink.probe[(Boolean, PagingInfo)]

      val probe = Source
        .single((false, emptyPagingInfo))
        .via(EndOfStreamDetector())
        .runWith(sinkProbe)

      probe.expectSubscriptionAndComplete()
    }

    "forward input if retry is true, no matter the page token" in {
      val sinkProbe = TestSink.probe[(Boolean, PagingInfo)]

      val probe = Source
        .single((true, emptyPagingInfo))
        .via(EndOfStreamDetector())
        .runWith(sinkProbe)

      probe.requestNext() should be((true, emptyPagingInfo))
    }

    "forward input if page token exists, even if retry is false" in {
      val sinkProbe = TestSink.probe[(Boolean, PagingInfo)]

      val probe = Source
        .single((false, pagingInfoWithPageToken))
        .via(EndOfStreamDetector())
        .runWith(sinkProbe)

      probe.requestNext() should be((false, pagingInfoWithPageToken))
    }

  }

}
