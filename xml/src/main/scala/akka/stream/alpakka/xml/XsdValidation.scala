/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.xml

import java.io.InputStream
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory

import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.util.{ ByteString, ByteStringBuilder }
import akka.{ Done, NotUsed }

import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

final case class XsdValidationResult(status: Try[Done])

object XsdValidation {
  /**
   * Returns an XsdValidation flow that returns an XsdValidationResult
   */
  def flow(xsdFromClassPath: String): Flow[ByteString, XsdValidationResult, NotUsed] =
    Flow.fromGraph(new XsdValidationFlow(xsdFromClassPath))

  /**
   * Creates an XsdValidation sink that materializes to a Future[XsdValidationResult]
   */
  def sink(xsdFromClassPath: String): Sink[ByteString, Future[XsdValidationResult]] =
    flow(xsdFromClassPath).toMat(Sink.head)(Keep.right)
}

private[xml] class XsdValidationFlow(xsdFromClassPath: String) extends GraphStage[FlowShape[ByteString, XsdValidationResult]] {
  val in: Inlet[ByteString] = Inlet("DigestCalculator.in")
  val out: Outlet[XsdValidationResult] = Outlet("DigestCalculator.out")
  override val shape: FlowShape[ByteString, XsdValidationResult] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val b = new ByteStringBuilder

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        b.append(grab(in))
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        try {
          val xsdStream: InputStream = getClass.getClassLoader.getResourceAsStream(xsdFromClassPath)
          val schemaLang = "http://www.w3.org/2001/XMLSchema"
          val factory = SchemaFactory.newInstance(schemaLang)
          val schema = factory.newSchema(new StreamSource(xsdStream))
          val validator = schema.newValidator()
          validator.validate(new StreamSource(b.result().iterator.asInputStream))
          emit(out, XsdValidationResult(Success(Done)))
        } catch {
          case t: Throwable =>
            emit(out, XsdValidationResult(Failure(t)))
        }
        completeStage()
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })
  }
}
