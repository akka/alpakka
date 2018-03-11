/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.{AmazonWebServiceResult, ResponseMetadata}
import SqsBatchAckFlowStage._
import com.amazonaws.services.sqs.model.{ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry}

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

      val partitionByAction = builder.add(Partition[MessageActionPair](3, {
        case (_, MessageAction.Delete) => 0
        case (_, MessageAction.Ignore) => 1
        case (_, MessageAction.ChangeMessageVisibility(_)) => 2
      }))
      def groupingStage(call: Call) =
        builder.add(Flow[MessageActionPair].groupedWithin(settings.maxBatchSize, settings.maxBatchWait).map(_ -> call))
      val mergeBatches = builder.add(Merge[(Iterable[MessageActionPair], Call)](3))
      val sqsStage = builder.add(new SqsBatchAckFlowStage[MessageActionPair](queueUrl, sqsClient))
      val flattenFutures = builder.add(Flow[Future[List[AckResult]]].mapAsync(settings.concurrentRequests)(identity))
      val flattenResults = builder.add(Flow[List[AckResult]].mapConcat(identity))

      partitionByAction.out(0) ~> groupingStage(SqsBatchAckFlowStage.Delete) ~> mergeBatches ~> sqsStage ~> flattenFutures ~> flattenResults
      partitionByAction.out(1) ~> groupingStage(SqsBatchAckFlowStage.Ignore) ~> mergeBatches
      partitionByAction.out(2) ~> groupingStage(SqsBatchAckFlowStage.ChangeMessageVisibility) ~> mergeBatches

      FlowShape(partitionByAction.in, flattenResults.out)
    }

    Flow.fromGraph(graph)
  }

  implicit val mBody: MessageActionPair => String = _._1.getBody
  implicit val deleteMessageEntryBuilder: MessageActionPair => DeleteMessageBatchRequestEntry =
    pair => new DeleteMessageBatchRequestEntry().withReceiptHandle(pair._1.getReceiptHandle)
  implicit val changeVisibilityEntryBuilder
    : PartialFunction[MessageActionPair, ChangeMessageVisibilityBatchRequestEntry] = {
    case (message, MessageAction.ChangeMessageVisibility(visibilityTimeout)) =>
      new ChangeMessageVisibilityBatchRequestEntry()
        .withReceiptHandle(message.getReceiptHandle)
        .withVisibilityTimeout(visibilityTimeout)
  }

}

/**
 * Messages returned by a SqsFlow.
 * @param metadata metadata with AWS response details.
 * @param message message body.
 */
final case class AckResult(
    metadata: Option[AmazonWebServiceResult[ResponseMetadata]],
    message: String
)
