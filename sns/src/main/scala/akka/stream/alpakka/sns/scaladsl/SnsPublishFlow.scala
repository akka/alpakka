/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sns.scaladsl

import akka.NotUsed
import akka.stream.alpakka.sns.SnsPublishFlowStage
import akka.stream.scaladsl.Flow
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sns.model.PublishResult

object SnsPublishFlow {

  /**
   * Scala API: creates a [[SnsPublishFlowStage]] for a SNS topic using an [[AmazonSNSAsync]]
   */
  def apply(topicArn: String)(implicit snsClient: AmazonSNSAsync): Flow[String, PublishResult, NotUsed] =
    Flow.fromGraph(new SnsPublishFlowStage(topicArn, snsClient))

}
