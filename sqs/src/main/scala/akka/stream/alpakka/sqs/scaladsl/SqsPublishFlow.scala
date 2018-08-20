/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.alpakka.sqs.impl.{SqsBatchFlowStage, SqsFlowStage}
import akka.stream.alpakka.sqs.{
  SqsPublishResult, SqsPublishBatchSettings, SqsPublishGroupedSettings, SqsPublishSettings}
import akka.stream.scaladsl.{Flow, GraphDSL}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.SendMessageRequest

import scala.concurrent.Future

/**
 * Scala API to create SQS flows.
 */
object SqsPublishFlow {

  /**
   * Creates a flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def apply(queueUrl: String, settings: SqsPublishSettings = SqsPublishSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] =
    Flow.fromGraph(new SqsFlowStage(queueUrl, sqsClient)).mapAsync(settings.maxInFlight)(identity)

  def grouped(queueUrl: String, settings: SqsPublishGroupedSettings = SqsPublishGroupedSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[SendMessageRequest, SqsPublishResult, NotUsed] = {
    val graph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val groupingStage: FlowShape[SendMessageRequest, Seq[SendMessageRequest]] =
        builder.add(Flow[SendMessageRequest].groupedWithin(settings.maxBatchSize, settings.maxBatchWait))

      val sqsStage: FlowShape[Seq[SendMessageRequest], Future[List[SqsPublishResult]]] =
        builder.add(new SqsBatchFlowStage(queueUrl, sqsClient))

      val flattenFutures: FlowShape[Future[List[SqsPublishResult]], List[SqsPublishResult]] =
        builder.add(Flow[Future[List[SqsPublishResult]]].mapAsync(settings.concurrentRequests)(identity))

      val flattenResults: FlowShape[List[SqsPublishResult], SqsPublishResult] = builder.add(Flow[List[SqsPublishResult]].mapConcat(identity))
      groupingStage ~> sqsStage ~> flattenFutures ~> flattenResults

      FlowShape(groupingStage.in, flattenResults.out)
    }

    Flow.fromGraph(graph)
  }

  def batch(queueUrl: String, settings: SqsPublishBatchSettings = SqsPublishBatchSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[Iterable[SendMessageRequest], List[SqsPublishResult], NotUsed] =
    Flow.fromGraph(new SqsBatchFlowStage(queueUrl, sqsClient)).mapAsync(settings.concurrentRequests)(identity)
}
