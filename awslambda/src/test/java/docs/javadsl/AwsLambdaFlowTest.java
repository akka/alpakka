/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.awslambda.javadsl.AwsLambdaFlow;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import org.junit.*;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AwsLambdaFlowTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static ActorMaterializer materializer;
  private static LambdaAsyncClient awsLambdaClient;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    awsLambdaClient = mock(LambdaAsyncClient.class);
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
    InvokeRequest invokeRequest = InvokeRequest.builder().build();
    InvokeResponse invokeResponse = InvokeResponse.builder().build();
    when(awsLambdaClient.invoke(eq(invokeRequest)))
        .thenAnswer(
            invocation -> {
              return CompletableFuture.completedFuture(invokeResponse);
            });
    Flow<InvokeRequest, InvokeResponse, NotUsed> flow = AwsLambdaFlow.create(awsLambdaClient, 1);
    Source<InvokeRequest, NotUsed> source = Source.single(invokeRequest);
    final CompletionStage<List<InvokeResponse>> stage =
        source.via(flow).runWith(Sink.seq(), materializer);
    assertEquals(1, stage.toCompletableFuture().get(3, TimeUnit.SECONDS).size());
  }
}
