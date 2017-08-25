/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.internal.stage

import akka.stream.alpakka.neo4j.internal.CypherMarshaller
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.neo4j.driver.v1.Driver

private[neo4j] class Neo4jFlowStage[A](driver: Driver, cypher: CypherMarshaller[A])
    extends GraphStage[FlowShape[A, A]] {

  private val in = Inlet[A]("neo4jFlow.in")
  private val out = Outlet[A]("neo4jFlow.out")

  override def shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {

      override protected def logSource = classOf[Neo4jFlowStage[A]]

      private val session = driver.session()

      setHandler(out, new OutHandler {
        override def onPull() =
          pull(in)
      })

      setHandler(in, new InHandler {
        override def onPush() = {
          val msg = grab(in)

          session.run(cypher.create(msg))

          push(out, msg)
        }

      })

      override def postStop() = {
        log.debug("Stage stopped")
        session.close()
      }

    }

}
