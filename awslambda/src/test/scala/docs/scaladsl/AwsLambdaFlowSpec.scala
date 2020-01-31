/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.CompletableFuture

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.awslambda.scaladsl.AwsLambdaFlow
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestKit
import org.mockito.ArgumentMatchers.{any => mockitoAny, eq => mockitoEq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.lambda.LambdaAsyncClient
import software.amazon.awssdk.services.lambda.model.{InvokeRequest, InvokeResponse}

import scala.concurrent.Await
import scala.concurrent.duration._

class AwsLambdaFlowSpec
    extends TestKit(ActorSystem("AwsLambdaFlowSpec"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures
    with Matchers
    with MockitoSugar
    with LogCapturing {

  implicit val mat = ActorMaterializer()

  implicit val ec = system.dispatcher

  implicit val awsLambdaClient = mock[LambdaAsyncClient]

  override protected def afterEach(): Unit = {
    reset(awsLambdaClient)
    verifyNoMoreInteractions(awsLambdaClient)
  }

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "AwsLambdaFlow" should {

    val invokeRequest =
      InvokeRequest.builder.functionName("test_function").payload(SdkBytes.fromUtf8String("test_payload")).build
    val invokeFailureRequest =
      InvokeRequest.builder.functionName("failure_function").payload(SdkBytes.fromUtf8String("test_payload")).build
    val invokeResponse = InvokeResponse.builder.build
    val lambdaFlow = AwsLambdaFlow(1)

    "call a single invoke request" in assertAllStagesStopped {

      when(
        awsLambdaClient.invoke(mockitoEq(invokeRequest))
      ).thenAnswer(new Answer[CompletableFuture[InvokeResponse]] {
        override def answer(invocation: InvocationOnMock): CompletableFuture[InvokeResponse] =
          CompletableFuture.completedFuture(invokeResponse)
      })

      val (probe, future) = TestSource.probe[InvokeRequest].via(lambdaFlow).toMat(Sink.seq)(Keep.both).run()
      probe.sendNext(invokeRequest)
      probe.sendComplete()

      Await.result(future, 3.seconds) shouldBe Vector(invokeResponse)
      verify(awsLambdaClient, times(1)).invoke(mockitoEq(invokeRequest))

    }

    "call with exception" in assertAllStagesStopped {

      when(
        awsLambdaClient.invoke(mockitoAny[InvokeRequest]())
      ).thenAnswer(new Answer[CompletableFuture[InvokeResponse]] {
        override def answer(invocation: InvocationOnMock): CompletableFuture[InvokeResponse] = {
          val exception = new RuntimeException("Error in lambda")
          val future = new CompletableFuture[InvokeResponse]()
          future.completeExceptionally(exception)
          future
        }
      })

      val (probe, future) = TestSource.probe[InvokeRequest].via(lambdaFlow).toMat(Sink.seq)(Keep.both).run()

      probe.sendNext(invokeFailureRequest)
      probe.sendComplete()

      val ex = Await.result(future.failed, 3.seconds)
      ex shouldBe a[RuntimeException]
      ex.getMessage shouldBe ("Error in lambda")
    }

  }
}
