/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.impl

import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.{ActorTestKit, BehaviorTestKit, TestInbox}
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.alpakka.mqtt.streaming.impl.QueueOfferState.QueueOfferCompleted
import akka.stream.QueueOfferResult
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class QueueOfferStateSpec
    extends TestKit(ActorSystem("QueueOfferStateSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with LogCapturing {

  sealed trait Msg

  case class DoubleIt(n: Int, reply: ActorRef[Int]) extends Msg
  case object NotHandled extends Msg
  case class Done(result: Either[Throwable, QueueOfferResult]) extends Msg with QueueOfferCompleted

  private implicit val ec: ExecutionContext = system.dispatcher

  private val baseBehavior = Behaviors.receivePartial[Msg] { case (context, DoubleIt(n, reply)) =>
    reply.tell(n * 2)

    Behaviors.same
  }

  "waitForQueueOfferCompleted" should {
    "work when immediately enqueued" in {
      val behavior = QueueOfferState.waitForQueueOfferCompleted[Msg](
        Future.successful(QueueOfferResult.Enqueued),
        r => Done(r.toEither),
        baseBehavior,
        Vector.empty
      )

      val testKit = BehaviorTestKit(behavior)

      val inbox = TestInbox[Int]()

      testKit.run(DoubleIt(2, inbox.ref))

      inbox.expectMessage(4)
    }

    "work when enqueued after some time" in {
      val done = Promise[QueueOfferResult]

      val behavior = QueueOfferState.waitForQueueOfferCompleted[Msg](
        done.future,
        r => Done(r.toEither),
        baseBehavior,
        Vector.empty
      )

      val testKit = ActorTestKit()
      val actor = testKit.spawn(behavior)
      val probe = testKit.createTestProbe[Int]()

      actor ! DoubleIt(2, probe.ref)

      system.scheduler.scheduleOnce(500.millis) {
        done.success(QueueOfferResult.Enqueued)
      }

      probe.expectMessage(5.seconds, 4)
    }

    "work when unhandled" in {
      val done = Promise[QueueOfferResult]

      val behavior = QueueOfferState.waitForQueueOfferCompleted[Msg](
        done.future,
        r => Done(r.toEither),
        baseBehavior,
        Vector.empty
      )

      val testKit = ActorTestKit()
      val actor = testKit.spawn(behavior)
      val probe = testKit.createTestProbe[Int]()

      actor ! NotHandled
      actor ! DoubleIt(4, probe.ref)

      system.scheduler.scheduleOnce(500.millis) {
        done.success(QueueOfferResult.Enqueued)
      }

      probe.expectMessage(8)
    }
  }
}
