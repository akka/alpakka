/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.awslambda.javadsl

import akka.NotUsed
import akka.stream.alpakka.awslambda.impl.AwsLambdaFlowStage
import akka.stream.javadsl.Flow
import com.amazonaws.services.lambda.model.{InvokeRequest, InvokeResult}
import com.amazonaws.services.lambda.AWSLambdaAsync

object AwsLambdaFlow {

  /**
   * Java API: creates a [[AwsLambdaFlowStage]] for a AWS Lambda function invocation using an [[AWSLambdaAsync]]
   */
  def create(awsLambdaClient: AWSLambdaAsync, parallelism: Int): Flow[InvokeRequest, InvokeResult, NotUsed] =
    Flow.fromGraph(new AwsLambdaFlowStage(awsLambdaClient)(parallelism))

}
