/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sns.javadsl

import akka.NotUsed
import akka.stream.alpakka.sns.SnsPublishFlowStage
import akka.stream.javadsl.Flow
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sns.model.PublishResult

object SnsPublishFlow {

  /**
   * Java API: creates a [[SnsPublishFlowStage]] for a SNS topic using an [[AmazonSNSAsync]]
   */
  def create(topicArn: String, snsClient: AmazonSNSAsync): Flow[String, PublishResult, NotUsed] =
    Flow.fromGraph(new SnsPublishFlowStage(topicArn, snsClient))

}
