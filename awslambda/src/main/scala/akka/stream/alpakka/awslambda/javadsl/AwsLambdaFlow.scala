/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.awslambda.javadsl

import akka.NotUsed
import akka.stream.alpakka.awslambda.AwsLambdaFlowStage
import akka.stream.javadsl.Flow
import com.amazonaws.services.lambda.model.{InvokeRequest, InvokeResult}
import com.amazonaws.services.lambda.{AWSLambdaAsyncClient, AWSLambdaClient}

object AwsLambdaFlow {

  /**
   * Java API: creates a [[AwsLambdaFlowStage]] for a AWS Lambda function invocation using an [[AWSLambdaClient]]
   */
  def create(awsLambdaClient: AWSLambdaAsyncClient, parallelism: Int): Flow[InvokeRequest, InvokeResult, NotUsed] =
    Flow.fromGraph(new AwsLambdaFlowStage(awsLambdaClient)(parallelism))

}
