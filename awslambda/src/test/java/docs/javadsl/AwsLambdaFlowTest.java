/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.awslambda.javadsl.AwsLambdaFlow;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.lambda.AWSLambdaAsyncClient;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AwsLambdaFlowTest {

  private static ActorSystem system;
  private static ActorMaterializer materializer;
  private static AWSLambdaAsyncClient awsLambdaClient;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    awsLambdaClient = mock(AWSLambdaAsyncClient.class);
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void lambdaFlow() throws Exception {
    InvokeRequest invokeRequest = new InvokeRequest();
    InvokeResult invokeResult = new InvokeResult();
    when(awsLambdaClient.invokeAsync(eq(invokeRequest), any()))
        .thenAnswer(
            invocation -> {
              AsyncHandler<InvokeRequest, InvokeResult> handler = invocation.getArgument(1);
              handler.onSuccess(invokeRequest, invokeResult);
              return new CompletableFuture<>();
            });
    Flow<InvokeRequest, InvokeResult, NotUsed> flow = AwsLambdaFlow.create(awsLambdaClient, 1);
    Source<InvokeRequest, NotUsed> source = Source.single(invokeRequest);
    final CompletionStage<List<InvokeResult>> stage =
        source.via(flow).runWith(Sink.seq(), materializer);
    assertEquals(1, stage.toCompletableFuture().get(3, TimeUnit.SECONDS).size());
  }
}
