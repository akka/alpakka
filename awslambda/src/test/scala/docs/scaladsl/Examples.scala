/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.awslambda.scaladsl.AwsLambdaFlow
import akka.stream.scaladsl.{Sink, Source}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.lambda.LambdaAsyncClient
import software.amazon.awssdk.services.lambda.model.InvokeRequest
import software.amazon.awssdk.core.SdkBytes

object Examples {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-client
  val credentials = AwsBasicCredentials.create("x", "x")
  implicit val lambdaClient: LambdaAsyncClient = LambdaAsyncClient
    .builder()
    .credentialsProvider(StaticCredentialsProvider.create(credentials))
    .build()
  //#init-client

  //#run
  val request = InvokeRequest
    .builder()
    .functionName("lambda-function-name")
    .payload(SdkBytes.fromUtf8String("test-payload"))
    .build()
  Source.single(request).via(AwsLambdaFlow(1)).runWith(Sink.seq)
  //#run
}
