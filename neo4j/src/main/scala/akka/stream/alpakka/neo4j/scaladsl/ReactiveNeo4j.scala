/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.scaladsl

import akka.stream.alpakka.neo4j.internal.codec.{Neo4jDecoder, Neo4jEncoder}
import akka.stream.alpakka.neo4j.internal.stage.{Neo4jFiniteSourceStage, Neo4jFlowStage}
import akka.stream.alpakka.neo4j.internal.{CypherMarshaller, CypherUnmarshaller, Neo4jCapabilities}
import akka.stream.scaladsl.{Flow, Source}
import akka.{Done, NotUsed}
import org.neo4j.driver.v1.{Driver, Statement, Values}

import scala.concurrent.Future

class ReactiveNeo4j(driver: Driver) extends Neo4jCapabilities(driver) {

  /**
   * Shapeless powered flow.
   * @param label given to nodes
   * @param encoder shapeless encoder
   */
  def flow[A](label: Symbol)(implicit encoder: Neo4jEncoder[A]): Flow[A, A, NotUsed] =
    Flow.fromGraph(new Neo4jFlowStage[A](driver, new ShapelessCypherMarshaller(label.name, encoder)))

  /**
   * Flow with hand crafted marshaller
   * @param writeHelper
   * @param label given to node
   */
  def flow[A](writeHelper: CypherMarshaller[A], label: Symbol): Flow[A, A, NotUsed] =
    Flow.fromGraph(new Neo4jFlowStage[A](driver, writeHelper))

  /**
   * Shapeless powered source.
   * @param decoder shapeless decoder
   */
  def source[A](statement: Statement)(implicit decoder: Neo4jDecoder[A]): Source[A, Future[Done]] =
    Source.fromGraph(new Neo4jFiniteSourceStage(driver, new ShapelessCypherUnmarshaller[A](decoder), statement))

  /**
   * Shapeless powered source.
   * @param decoder shapeless decoder
   */
  def source[A](statement: String, args: String*)(implicit decoder: Neo4jDecoder[A]): Source[A, Future[Done]] =
    source(new Statement(statement, Values.parameters(args: _*)))(decoder)

  /**
   * Source with hand crafted unmarshaller
   * @param readedHelper
   */
  def source[A](readedHelper: CypherUnmarshaller[A], statement: Statement): Source[A, Future[Done]] =
    Source.fromGraph(new Neo4jFiniteSourceStage(driver, readedHelper, statement))

  /**
   * Source with hand crafted unmarshaller
   * @param readedHelper
   */
  def source[A](readedHelper: CypherUnmarshaller[A], statement: String, args: String*): Source[A, Future[Done]] =
    source(readedHelper, new Statement(statement, Values.parameters(args: _*)))

}
