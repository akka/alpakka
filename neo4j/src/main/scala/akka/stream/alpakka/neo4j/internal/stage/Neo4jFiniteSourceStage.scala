/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.internal.stage

import akka.Done
import akka.stream.alpakka.neo4j.internal.CypherUnmarshaller
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import org.neo4j.driver.v1._

import scala.concurrent.{Future, Promise}

class Neo4jFiniteSourceStage[A](driver: Driver, reader: CypherUnmarshaller[A], statement: Statement)
    extends GraphStageWithMaterializedValue[SourceShape[A], Future[Done]] {

  override protected def initialAttributes: Attributes =
    Attributes.name("Neo4jFiniteSource").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))

  val out = Outlet[A]("neo4j.finiteSource")

  override def shape: SourceShape[A] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val subPromise = Promise[Done]
    (new GraphStageLogic(shape) {
      var session: Session = _

      var result: StatementResult = _

      override def preStart(): Unit = {
        session = driver.session()
        result = session.run(statement)
        subPromise.success(Done)
      }

      override def postStop(): Unit =
        session.close()

      setHandler(
        out,
        new OutHandler {
          override def onPull() =
            if (result.hasNext) {
              Option(reader.unmarshall(result.next().get(0))).foreach { a =>
                push(out, a)
              }
            } else
              completeStage()
        }
      )

    }, subPromise.future)
  }
}
