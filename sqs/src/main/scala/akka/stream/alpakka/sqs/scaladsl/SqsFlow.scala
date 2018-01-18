/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.alpakka.sqs.{SqsBatchFlowSettings, SqsBatchFlowStage, SqsFlowStage, SqsSinkSettings}
import akka.stream.scaladsl.{Flow, GraphDSL}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.SendMessageResult

import scala.concurrent.Future

object SqsFlow {

  /**
   * Scala API: creates a [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
  def apply(queueUrl: String, settings: SqsSinkSettings = SqsSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[String, Result, NotUsed] =
    Flow.fromGraph(new SqsFlowStage(queueUrl, sqsClient)).mapAsync(settings.maxInFlight)(identity)

  def grouped(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[String, Result, NotUsed] = {
    val graph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val groupingStage: FlowShape[String, Seq[String]] =
        builder.add(Flow[String].groupedWithin(settings.maxBatchSize, settings.maxBatchWait))

      val sqsStage: FlowShape[Seq[String], Future[List[Result]]] =
        builder.add(new SqsBatchFlowStage(queueUrl, sqsClient))

      val flattenFutures: FlowShape[Future[List[Result]], List[Result]] =
        builder.add(Flow[Future[List[Result]]].mapAsync(settings.concurrentRequests)(identity))

      val flattenResults: FlowShape[List[Result], Result] = builder.add(Flow[List[Result]].mapConcat(identity))
      groupingStage ~> sqsStage ~> flattenFutures ~> flattenResults

      FlowShape(groupingStage.in, flattenResults.out)
    }

    Flow.fromGraph(graph)
  }

  def batch(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[Iterable[String], List[Result], NotUsed] =
    Flow.fromGraph(new SqsBatchFlowStage(queueUrl, sqsClient)).mapAsync(settings.concurrentRequests)(identity)
}

/**
 * Messages returned by a SqsFlow.
 * @param metadata metadata with AWS response details.
 * @param message message body.
 */
final case class Result(
    metadata: SendMessageResult,
    message: String
)
