/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.awslambda.scaladsl.AwsLambdaFlow
import akka.stream.scaladsl.{Sink, Source}
import software.amazon.awssdk.services.lambda.LambdaAsyncClient

object Examples {

  //#init-mat
  import akka.actor.ActorSystem
  import akka.stream.{ActorMaterializer, Materializer}

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  //#init-mat

  def initClient(): Unit = {
    //#init-client
    import com.github.matsluni.akkahttpspi.AkkaHttpClient
    import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
    import software.amazon.awssdk.services.lambda.LambdaAsyncClient

    // Don't encode credentials in your source code!
    // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
    implicit val lambdaClient: LambdaAsyncClient = LambdaAsyncClient
      .builder()
      .credentialsProvider(credentialsProvider)
      .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
      // Possibility to configure the retry policy
      // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
      // .overrideConfiguration(...)
      .build()

    system.registerOnTermination(lambdaClient.close())
    //#init-client
  }

  def run()(implicit lambdaClient: LambdaAsyncClient): Unit = {
    //#run
    import software.amazon.awssdk.core.SdkBytes
    import software.amazon.awssdk.services.lambda.model.InvokeRequest

    val request = InvokeRequest
      .builder()
      .functionName("lambda-function-name")
      .payload(SdkBytes.fromUtf8String("test-payload"))
      .build()
    Source.single(request).via(AwsLambdaFlow(1)).runWith(Sink.seq)
    //#run
  }
}
