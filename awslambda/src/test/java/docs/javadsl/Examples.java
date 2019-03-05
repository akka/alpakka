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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.core.SdkBytes;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;

public class Examples {

  // #init-mat
  ActorSystem system = ActorSystem.create();
  ActorMaterializer materializer = ActorMaterializer.create(system);
  // #init-mat

  // #init-client
  AwsBasicCredentials credentials = AwsBasicCredentials.create("x", "x");
  LambdaAsyncClient awsLambdaClient =
      LambdaAsyncClient.builder()
          .credentialsProvider(StaticCredentialsProvider.create(credentials))
          .build();
  // #init-client

  // #run
  InvokeRequest request =
      InvokeRequest.builder()
          .functionName("lambda-function-name")
          .payload(SdkBytes.fromUtf8String("test-payload"))
          .build();
  Flow<InvokeRequest, InvokeResponse, NotUsed> flow = AwsLambdaFlow.create(awsLambdaClient, 1);
  final CompletionStage<List<InvokeResponse>> stage =
      Source.single(request).via(flow).runWith(Sink.seq(), materializer);
  // #run
}
