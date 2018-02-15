/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.alpakka.sqs.{SqsBatchFlowSettings, SqsBatchFlowStage, SqsFlowStage, SqsSinkSettings}
import akka.stream.scaladsl.{Flow, GraphDSL}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{SendMessageRequest, SendMessageResult}

import scala.concurrent.Future

/**
 * Scala API to create SQS flows.
 */
object SqsFlow {

  /**
   * Creates a flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def apply(queueUrl: String, settings: SqsSinkSettings = SqsSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, Result, NotUsed] =
    Flow.fromGraph(new SqsFlowStage(queueUrl, sqsClient)).mapAsync(settings.maxInFlight)(identity)

  def grouped(queueUrl: String, settings: SqsBatchFlowSettings = SqsBatchFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, Result, NotUsed] = {
    val graph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val groupingStage: FlowShape[SendMessageRequest, Seq[SendMessageRequest]] =
        builder.add(Flow[SendMessageRequest].groupedWithin(settings.maxBatchSize, settings.maxBatchWait))

      val sqsStage: FlowShape[Seq[SendMessageRequest], Future[List[Result]]] =
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
  ): Flow[Iterable[SendMessageRequest], List[Result], NotUsed] =
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
