/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.alpakka.sqs.MessageAction.ChangeMessageVisibility
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.impl.{SqsAckFlowStage, SqsBatchChangeMessageVisibilityFlowStage, SqsBatchDeleteFlowStage}
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.Message

import scala.concurrent.Future

/**
 * Scala API to create acknowledging SQS flows.
 */
object SqsAckFlow {

  /**
   * Creates flow for a SQS queue using an [[com.amazonaws.services.sqs.AmazonSQSAsync]].
   */
  def apply(queueUrl: String, settings: SqsAckSettings = SqsAckSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[MessageAction, SqsAckResult, NotUsed] =
    Flow.fromGraph(new SqsAckFlowStage(queueUrl, sqsClient)).mapAsync(settings.maxInFlight)(identity)

  def grouped(queueUrl: String, settings: SqsAckGroupedSettings = SqsAckGroupedSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[MessageAction, SqsAckResult, NotUsed] = {
    val graph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val partitionByAction = builder.add(Partition[MessageAction](3, {
        case _: MessageAction.Delete => 0
        case _: MessageAction.ChangeMessageVisibility => 1
        case _: MessageAction.Ignore => 2
      }))

      def groupingStage[A] = Flow[A].groupedWithin(settings.maxBatchSize, settings.maxBatchWait)

      val sqsDeleteStage = new SqsBatchDeleteFlowStage(queueUrl, sqsClient)
      val sqsChangeVisibilityStage = new SqsBatchChangeMessageVisibilityFlowStage(queueUrl, sqsClient)
      val flattenFutures = Flow[Future[List[SqsAckResult]]].mapAsync(settings.concurrentRequests)(identity)
      val ignore = Flow[MessageAction].map(x => Future.successful(List(SqsAckResult(None, x.message.getBody))))
      val getMessage = Flow[MessageAction].map(_.message)
      val getMessageChangeVisibility =
        Flow[MessageAction].map(_.asInstanceOf[ChangeMessageVisibility])

      val mergeResults = builder.add(Merge[Future[List[SqsAckResult]]](3))
      val flattenResults = builder.add(Flow[List[SqsAckResult]].mapConcat(identity))

      partitionByAction.out(0) ~> getMessage ~> groupingStage[Message] ~> sqsDeleteStage ~> mergeResults ~> flattenFutures ~> flattenResults
      partitionByAction.out(1) ~> getMessageChangeVisibility ~> groupingStage[ChangeMessageVisibility] ~> sqsChangeVisibilityStage ~> mergeResults
      partitionByAction.out(2) ~> ignore ~> mergeResults

      FlowShape(partitionByAction.in, flattenResults.out)
    }

    Flow.fromGraph(graph)
  }
}
