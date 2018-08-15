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
  def apply(queueUrl: String, settings: SqsAckSinkSettings = SqsAckSinkSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[MessageActionPair, AckResult, NotUsed] =
    Flow.fromGraph(new SqsAckFlowStage(queueUrl, sqsClient)).mapAsync(settings.maxInFlight)(identity)

  def grouped(queueUrl: String, settings: SqsBatchAckFlowSettings = SqsBatchAckFlowSettings.Defaults)(
      implicit sqsClient: AmazonSQSAsync
  ): Flow[MessageActionPair, AckResult, NotUsed] = {
    val graph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val partitionByAction = builder.add(Partition[MessageActionPair](3, _.action match {
        case MessageAction.Delete => 0
        case MessageAction.ChangeMessageVisibility(_) => 1
        case MessageAction.Ignore => 2
      }))

      def groupingStage[A] = Flow[A].groupedWithin(settings.maxBatchSize, settings.maxBatchWait)

      val sqsDeleteStage = new SqsBatchDeleteFlowStage(queueUrl, sqsClient)
      val sqsChangeVisibilityStage = new SqsBatchChangeMessageVisibilityFlowStage(queueUrl, sqsClient)
      val flattenFutures = Flow[Future[List[AckResult]]].mapAsync(settings.concurrentRequests)(identity)
      val ignore = Flow[MessageActionPair].map(x => Future.successful(List(AckResult(None, x.message.getBody))))
      val getMessage = Flow[MessageActionPair].map(_.message)
      val getMessageChangeVisibility =
        Flow[MessageActionPair].map(m => (m.message, m.action.asInstanceOf[ChangeMessageVisibility]))

      val mergeResults = builder.add(Merge[Future[List[AckResult]]](3))
      val flattenResults = builder.add(Flow[List[AckResult]].mapConcat(identity))

      partitionByAction.out(0) ~> getMessage ~> groupingStage[Message] ~> sqsDeleteStage ~> mergeResults ~> flattenFutures ~> flattenResults
      partitionByAction.out(1) ~> getMessageChangeVisibility ~> groupingStage[(Message, ChangeMessageVisibility)] ~> sqsChangeVisibilityStage ~> mergeResults
      partitionByAction.out(2) ~> ignore ~> mergeResults

      FlowShape(partitionByAction.in, flattenResults.out)
    }

    Flow.fromGraph(graph)
  }
}
