/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.aws.sagemaker.javadsl

import akka.NotUsed
import akka.stream.alpakka.aws.sagemaker.impl.SagemakerInvokeFlowStage
import akka.stream.javadsl.Flow
import software.amazon.awssdk.services.sagemakerruntime.SageMakerRuntimeAsyncClient
import software.amazon.awssdk.services.sagemakerruntime.model.{InvokeEndpointRequest, InvokeEndpointResponse}

object SagemakerInvokeFlow {

  /**
   * Java API: creates a [[SagemakerInvokeFlowStage]] for a AWS Lambda function invocation
   * using [[software.amazon.awssdk.services.sagemakerruntime.SageMakerRuntimeAsyncClient]]
   */
  def apply(parallelism: Int)(
      implicit sagemakerRuntimeClient: SageMakerRuntimeAsyncClient
  ): Flow[InvokeEndpointRequest, InvokeEndpointResponse, NotUsed] =
    Flow.fromGraph(new SagemakerInvokeFlowStage(sagemakerRuntimeClient)(parallelism))

}
