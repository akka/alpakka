/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.awslambda.scaladsl

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.lambda.AWSLambdaAsyncClient
import com.amazonaws.services.lambda.model.InvokeRequest

object Examples {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-client
  val credentials = new BasicAWSCredentials("x", "x")
  implicit val lambdaClient: AWSLambdaAsyncClient =
    new AWSLambdaAsyncClient(credentials, Executors.newFixedThreadPool(1))
  //#init-client

  //#run
  val request = new InvokeRequest().withFunctionName("lambda-function-name").withPayload("test-payload")
  Source.single(request).via(AwsLambdaFlow(1)).runWith(Sink.seq)
  //#run
}
