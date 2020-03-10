/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.cassandra.scaladsl

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.ExecutionContext
import akka.stream.{ActorMaterializer, Materializer}

/**
 * All the tests must be run with a local Cassandra running on default port 9042.
 */
abstract class CassandraSpecBase(_system: ActorSystem)
    extends TestKit(_system)
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers
    with CassandraLifecycle {

  implicit val materializer: Materializer = ActorMaterializer()(_system)
  implicit val ec: ExecutionContext = system.dispatcher

  lazy val sessionRegistry: CassandraSessionRegistry = CassandraSessionRegistry.get(system)

}
