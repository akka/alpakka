/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

// ORIGINAL LICENCE
/*
 *  Copyright 2017 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package akka.stream.alpakka.chroniclequeue

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph}
import akka.stream.{ActorMaterializer, ClosedShape}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await

import akka.stream.alpakka.chroniclequeue.impl.{CommitOrderException, Event}
import akka.stream.alpakka.chroniclequeue.scaladsl._
import Timeouts._

class ChronicleQueueCommitOrderSpec extends FlatSpec with Matchers with BeforeAndAfterAll with Eventually {

  implicit val system = ActorSystem("ChronicleQueueCommitOrderSpec", ChronicleQueueSpec.testConfig)
  implicit val mat = ActorMaterializer()
  implicit val serializer = ChronicleQueueSerializer[Int]()
  import StreamSpecUtil._

  override def afterAll =
    Await.ready(system.terminate(), awaitMax)

  it should "fail when an out of order commit is attempted and commit-order-policy = strict" in {
    val util = new StreamSpecUtil[Int, Event[Int]]
    import util._
    val buffer =
      ChronicleQueueAtLeastOnce[Int](ConfigFactory.parseString("commit-order-policy = strict").withFallback(config))
    val commit = buffer.commit[Int]

    val streamGraph = RunnableGraph.fromGraph(GraphDSL.create(flowCounter) { implicit builder => sink =>
      import GraphDSL.Implicits._
      in ~> buffer.async ~> filterARandomElement ~> commit ~> sink
      ClosedShape
    })
    val sinkF = streamGraph.run()
    Await.result(sinkF.failed, awaitMax) shouldBe an[CommitOrderException]
    clean()
  }

  it should "not fail when an out of order commit is attempted and commit-order-policy = lenient" in {
    val util = new StreamSpecUtil[Int, Event[Int]]
    import util._
    val buffer =
      ChronicleQueueAtLeastOnce[Int](ConfigFactory.parseString("commit-order-policy = lenient").withFallback(config))
    val commit = buffer.commit[Int]

    val streamGraph = RunnableGraph.fromGraph(GraphDSL.create(flowCounter) { implicit builder => sink =>
      import GraphDSL.Implicits._
      in ~> buffer.async ~> filterARandomElement ~> commit ~> sink
      ClosedShape
    })

    val countFuture = streamGraph.run()
    val count = Await.result(countFuture, awaitMax)
    count shouldBe elementCount - 1
    eventually { buffer.queue shouldBe 'closed }

    clean()
  }
}
