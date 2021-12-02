/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.impl

import akka.stream.alpakka.ironmq.{IronMqSpec, PushMessage}
import org.scalatest.concurrent.Eventually

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class IronMqClientSpec extends IronMqSpec with Eventually {

  "IronMqClient" when {
    "listQueues is called" should {
      "return the list of queue" in {
        val queue = givenQueue("test-1")
        ironMqClient.listQueues().futureValue should contain(queue)
      }
    }

    "createQueue is called" should {
      "return the created queue" in {
        val queue = ironMqClient.createQueue("test-1").futureValue
        queue should be("test-1")
      }

      "create the queue" in {
        val queue = ironMqClient.createQueue("test-1").futureValue
        ironMqClient.listQueues().futureValue should contain only queue
      }
    }

    "deleteQueue is called" should {
      "delete the queue" in {
        val queue = givenQueue("test")
        ironMqClient.deleteQueue(queue).futureValue
        eventually(timeout(5.seconds), interval(1.second)) {
          ironMqClient.listQueues().futureValue should not contain queue
        }
      }
    }

    "pushMessages is called" should {
      "return messages ids" in {
        val queue = givenQueue()
        ironMqClient
          .pushMessages(queue, PushMessage("test-1"), PushMessage("test-2"))
          .futureValue
          .ids should have size 2
      }

      "push messages in the queue" in {
        val queue = givenQueue()
        ironMqClient.pushMessages(queue, PushMessage("test-1"), PushMessage("test-2")).futureValue.ids
        ironMqClient
          .pullMessages(queue, 20)
          .futureValue
          .map(_.body)
          .toSeq should contain inOrder ("test-1", "test-2")
      }
    }

    "pullMessages is called" should {
      "remove messages from queue" in {
        val queue = givenQueue()
        ironMqClient.pushMessages(queue, PushMessage("test-1"), PushMessage("test-2")).futureValue.ids
        ironMqClient
          .pullMessages(queue, 2)
          .futureValue
          .map(_.body)
          .toSeq should contain inOrder ("test-1", "test-2")
        ironMqClient.peekMessages(queue, 20).futureValue.toSeq should be(empty)
      }
    }

    "reserveMessages is called" should {
      "remove messages from queue temporarily" ignore { // It is too slow to run
        val queue = givenQueue()
        ironMqClient.pushMessages(queue, PushMessage("test-1"), PushMessage("test-2")).futureValue.ids
        ironMqClient.reserveMessages(queue, 2, 30.seconds).futureValue
        ironMqClient.peekMessages(queue, 20).futureValue.toSeq should be(empty)
        eventually(timeout(60.seconds), interval(15.seconds)) {
          ironMqClient.peekMessages(queue, 20).futureValue should have size 2
        }
      }
    }

    "releaseMessages is called" should {
      "release reserved messages" in {
        val queue = givenQueue()
        ironMqClient.pushMessages(queue, PushMessage("test-1")).futureValue
        val reservation = ironMqClient.reserveMessages(queue, 1, 30.seconds).map(_.head.reservation).futureValue
        ironMqClient.releaseMessage(queue, reservation).futureValue
        val msg = eventually(timeout(5.seconds), interval(1.second)) {
          ironMqClient.pullMessages(queue).futureValue.head
        }

        msg.body shouldBe "test-1"

      }
    }
  }

}
