/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.awslambda.scaladsl.AwsLambdaFlow
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.lambda.model.InvokeRequest
import com.amazonaws.services.lambda.{AWSLambdaAsync, AWSLambdaAsyncClientBuilder}

object Examples {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-client
  val credentials = new BasicAWSCredentials("x", "x")
  implicit val lambdaClient: AWSLambdaAsync = AWSLambdaAsyncClientBuilder
    .standard()
    .withCredentials(new AWSStaticCredentialsProvider(credentials))
    .build();
  //#init-client

  //#run
  val request = new InvokeRequest().withFunctionName("lambda-function-name").withPayload("test-payload")
  Source.single(request).via(AwsLambdaFlow(1)).runWith(Sink.seq)
  //#run
}
