/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.awslambda.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import software.amazon.awssdk.services.lambda.model.{InvokeRequest, InvokeResponse}
import software.amazon.awssdk.services.lambda.LambdaAsyncClient
import scala.compat.java8.FutureConverters._

object AwsLambdaFlow {

  /**
   * Scala API: creates a [[AwsLambdaFlowStage]] for a AWS Lambda function invocation using [[LambdaAsyncClient]]
   */
  def apply(
      parallelism: Int
  )(implicit awsLambdaClient: LambdaAsyncClient): Flow[InvokeRequest, InvokeResponse, NotUsed] =
    Flow[InvokeRequest].mapAsyncUnordered(parallelism)(awsLambdaClient.invoke(_).toScala)

}
