/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection

trait RedisSupport {

  //#init-actor-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  //#init-actor-system

  //#create-redis-connection
  val redisClient: RedisClient = RedisClient.create("redis://localhost:6379/0")
  val connection: StatefulRedisConnection[String, String] = redisClient.connect()
  //#create-redis-connection

  val pubSub: StatefulRedisPubSubConnection[String, String] = redisClient.connectPubSub()

  def beforeAll(): Unit = ()

  def afterAll(): Unit = {
    connection.close()
    redisClient.shutdown()

  }

}
