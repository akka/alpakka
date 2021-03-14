/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
// #init-sys
import akka.actor.ActorSystem;
// #init-sys
import akka.stream.alpakka.awslambda.javadsl.AwsLambdaFlow;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
// #init-client
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
// #init-client
// #run
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.core.SdkBytes;
// #run

import java.util.List;
import java.util.concurrent.CompletionStage;

public class Examples {

  // #init-sys

  ActorSystem system = ActorSystem.create();
  // #init-sys

  public void initClient() {
    // #init-client

    // Don't encode credentials in your source code!
    // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
    StaticCredentialsProvider credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"));
    LambdaAsyncClient awsLambdaClient =
        LambdaAsyncClient.builder()
            .credentialsProvider(credentialsProvider)
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            // Possibility to configure the retry policy
            // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
            // .overrideConfiguration(...)
            .build();

    system.registerOnTermination(awsLambdaClient::close);
    // #init-client
  }

  public void run(LambdaAsyncClient awsLambdaClient) {
    // #run

    InvokeRequest request =
        InvokeRequest.builder()
            .functionName("lambda-function-name")
            .payload(SdkBytes.fromUtf8String("test-payload"))
            .build();
    Flow<InvokeRequest, InvokeResponse, NotUsed> flow = AwsLambdaFlow.create(awsLambdaClient, 1);
    final CompletionStage<List<InvokeResponse>> stage =
        Source.single(request).via(flow).runWith(Sink.seq(), system);
    // #run
  }
}
