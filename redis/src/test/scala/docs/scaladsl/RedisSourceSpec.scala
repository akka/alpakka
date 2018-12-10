/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.{Sink, Source}
import akka.streams.alpakka.redis.{RedisFieldValue, RedisHMSet, RedisHSet, RedisKeyValue}
import akka.streams.alpakka.redis.scaladsl.{RedisFlow, RedisSource}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Random

class RedisSourceSpec extends Specification with BeforeAfterAll with RedisSupport {

  sequential

  "scaladsl.RedisFlow" should {

    "implement get command " in {

      val set = RedisKeyValue("key123", "value1")

      val result = Source
        .single(set)
        .via(RedisFlow.set(1, connection))
        .runWith(Sink.ignore)

      Await.result(result, Duration(5, TimeUnit.SECONDS))
      val getResultFuture = RedisSource.get[String, String](set.key, connection).runWith(Sink.head)
      val getResult = Await.result(getResultFuture, Duration(5, TimeUnit.SECONDS))
      getResult shouldEqual set

    }

    "implement mget command" in {

      val sets = Seq[RedisKeyValue[String, String]](RedisKeyValue("key1", "value1"),
                                                    RedisKeyValue("key2", "value2"),
                                                    RedisKeyValue("key3", "value3"))

      val result = Source
        .fromIterator(() => sets.iterator)
        .grouped(2)
        .via(RedisFlow.mset(2, connection))
        .runWith(Sink.ignore)

      Await.result(result, Duration(5, TimeUnit.SECONDS))

      val mgetResultFuture = RedisSource.mget[String, String](sets.map(_.key), connection).runWith(Sink.seq)
      val mgetResult = Await.result(mgetResultFuture, Duration(5, TimeUnit.SECONDS))
      mgetResult shouldEqual sets
    }

    "implement hget Command" in {

      val field = Random.alphanumeric.take(10).mkString("")

      connection.sync().hset("KEY1", field, "value1")
      val redisHSet = RedisHSet[String, String]("KEY1", field, "value1")

      val hgetResultFuture = RedisSource.hget("KEY1", field, connection).runWith(Sink.head)
      val hgetResult = Await.result(hgetResultFuture, Duration(5, TimeUnit.SECONDS))
      hgetResult shouldEqual redisHSet

    }

    "implement hmget" in {
      val key: String = "KEY3"
      val redisFieldValues = Seq[RedisFieldValue[String, String]](RedisFieldValue("field1", "value1"),
                                                                  RedisFieldValue("field2", "value2"),
                                                                  RedisFieldValue("field3", "value3"))

      val expectedResult: Seq[RedisKeyValue[String, String]] =
        redisFieldValues.map(f => RedisKeyValue(f.field, f.value))
      val redisHMSet: RedisHMSet[String, String] = RedisHMSet(key, redisFieldValues)

      val resultAsFuture = Source.single(redisHMSet).via(RedisFlow.hmset(1, connection)).runWith(Sink.head)
      Await.result(resultAsFuture, Duration(5, TimeUnit.SECONDS))

      val hmgetResultFuture = RedisSource.hmget(key, redisFieldValues.map(_.field), connection).runWith(Sink.seq)
      val result = Await.result(hmgetResultFuture, Duration(5, TimeUnit.SECONDS))
      result shouldEqual expectedResult

    }
  }
}
