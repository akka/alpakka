/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.awslambda.scaladsl

import akka.NotUsed
import akka.stream.alpakka.awslambda.AwsLambdaFlowStage
import akka.stream.scaladsl.Flow
import com.amazonaws.services.lambda.model.{InvokeRequest, InvokeResult}
import com.amazonaws.services.lambda.{AWSLambdaAsyncClient, AWSLambdaClient}

object AwsLambdaFlow {

  /**
   * Scala API: creates a [[AwsLambdaFlowStage]] for a AWS Lambda function invocation using [[AWSLambdaClient]]
   */
  def apply(
      parallelism: Int
  )(implicit awsLambdaClient: AWSLambdaAsyncClient): Flow[InvokeRequest, InvokeResult, NotUsed] =
    Flow.fromGraph(new AwsLambdaFlowStage(awsLambdaClient)(parallelism))

}
