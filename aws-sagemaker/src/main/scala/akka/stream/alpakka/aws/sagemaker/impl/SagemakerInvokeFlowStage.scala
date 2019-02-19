/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.aws.sagemaker.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.aws.AsyncHandlerFlowStage

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.sagemakerruntime.SageMakerRuntimeAsyncClient
import software.amazon.awssdk.services.sagemakerruntime.model.{InvokeEndpointRequest, InvokeEndpointResponse}

/**
 * INTERNAL API
 */
@InternalApi private[sagemaker] final class SagemakerInvokeFlowStage(sagemakerClient: SageMakerRuntimeAsyncClient)(
    parallelism: Int
) extends AsyncHandlerFlowStage[InvokeEndpointRequest, InvokeEndpointResponse](parallelism) {

  /**
   *
   * @param request
   * @return
   */
  override def clientInvoke(request: InvokeEndpointRequest): CompletableFuture[InvokeEndpointResponse] =
    sagemakerClient.invokeEndpoint(request)
}
