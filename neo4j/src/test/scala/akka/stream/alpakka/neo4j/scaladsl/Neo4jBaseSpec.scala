/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.slf4j.LoggerFactory

class Neo4jBaseSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  val logger = LoggerFactory.getLogger(classOf[Neo4jBaseSpec]);

  //#connect
  val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "test"))
  //#connect

  implicit val system = ActorSystem("test")
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    system.terminate()
    driver.close()
  }

}
