/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl

import akka.NotUsed
import akka.stream.alpakka.sqs.{scaladsl, MessageActionPair, SqsFlowStage, SqsSinkSettings}
import akka.stream.javadsl.Flow
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.SendMessageResult

object SqsAckFlow {

  /**
   * Java API: creates an acknowledging flow based on [[SqsFlowStage]] for a SQS queue using an [[AmazonSQSAsync]]
   */
//  def create(queueUrl: String,
//             settings: SqsSinkSettings,
//             sqsClient: AmazonSQSAsync): Flow[MessageActionPair, MessageActionPair, NotUsed] =
//    scaladsl.SqsFlow.apply(queueUrl, settings)(sqsClient).asJava

}
