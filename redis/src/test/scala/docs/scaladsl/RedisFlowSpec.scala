/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.streams.alpakka.redis.scaladsl.{RedisFlow, RedisSource}
import akka.streams.alpakka.redis.{RedisKeyValue, RedisKeyValues, RedisOperationResult, RedisPubSub}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Random

class RedisFlowSpec extends Specification with BeforeAfterAll with RedisSupport {

  sequential

  "scaladsl.RedisFlow" should {

    "insert single key/value" in assertAllStagesStopped {

      val key = Random.alphanumeric.take(10).mkString("")

      val result = Source
        .single(RedisKeyValue(key, "value1"))
        .via(RedisFlow.set(1, connection))
        .runWith(Sink.head[RedisOperationResult[RedisKeyValue[String, String], String]])
      Await.result(result, Duration(5, TimeUnit.SECONDS)).result.get shouldEqual "OK"
    }

    "append single value" in assertAllStagesStopped {

      val key = Random.alphanumeric.take(10).mkString("")
      val result = Source
        .single(RedisKeyValue(key, "value1"))
        .via(RedisFlow.append(1, connection))
        .runWith(Sink.head[RedisOperationResult[RedisKeyValue[String, String], Long]])
      Await.result(result, Duration(5, TimeUnit.SECONDS)).result.get shouldEqual 6L
    }

    "insert bulk of key/value" in assertAllStagesStopped {

      val sets = Seq[RedisKeyValue[String, String]](RedisKeyValue("key1", "value1"),
                                                    RedisKeyValue("key2", "value2"),
                                                    RedisKeyValue("key3", "value3"))

      val result = Source
        .fromIterator(() => sets.iterator)
        .grouped(2)
        .via(RedisFlow.mset(2, connection))
        .runWith(Sink.head[RedisOperationResult[Seq[RedisKeyValue[String, String]], String]])

      Await.result(result, Duration(5, TimeUnit.SECONDS)).result.isSuccess shouldEqual true
    }

    "insert multiple values to single key with lpush" in assertAllStagesStopped {

      val key = Random.alphanumeric.take(10).mkString("")

      val result = Source
        .single(RedisKeyValues(key, Array("1", "2", "3")))
        .via(RedisFlow.lpush(1, connection))
        .runWith(Sink.head[RedisOperationResult[RedisKeyValues[String, String], Long]])

      Await.result(result, Duration(50, TimeUnit.SECONDS)).result.get shouldEqual 3L

    }

    "implement pub/sub for single topic and return notfication of single consumer" in {
      val topic = "topic2"
      RedisSource.subscribe(Seq(topic), pubSub).runWith(Sink.ignore)

      val result = Source
        .single("Bla")
        .map(f => RedisPubSub(topic, f))
        .via(RedisFlow.publish[String, String](1, redisClient.connectPubSub().async().getStatefulConnection))
        .runWith(Sink.head[RedisOperationResult[RedisPubSub[String, String], Long]])

      Await.result(result, Duration(50, TimeUnit.SECONDS)).result.get shouldEqual 1L

    }

    "implement pub/sub for single topic and return published element from consumer" in assertAllStagesStopped {
      val topic = "topic2"

      val recievedMessage = RedisSource.subscribe(Seq(topic), pubSub).runWith(Sink.head[RedisPubSub[String, String]])

      Source
        .single("Bla")
        .map(f => RedisPubSub(topic, f))
        .via(RedisFlow.publish[String, String](1, redisClient.connectPubSub().async().getStatefulConnection))
        .runWith(Sink.head[RedisOperationResult[RedisPubSub[String, String], Long]])

      Await.result(recievedMessage, Duration(50, TimeUnit.SECONDS)) shouldEqual RedisPubSub(topic, "Bla")

    }

  }

}
