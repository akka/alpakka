/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.awslambda.scaladsl

import java.util.concurrent.{CompletableFuture, Future}

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestKit
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.lambda.AWSLambdaAsyncClient
import com.amazonaws.services.lambda.model.{InvokeRequest, InvokeResult}
import org.mockito.ArgumentMatchers.{any => mockitoAny, eq => mockitoEq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar

class AwsLambdaFlowSpec
    extends TestKit(ActorSystem("AwsLambdaFlowSpec"))
    with WordSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures
    with Matchers
    with MockitoSugar {

  implicit val mat = ActorMaterializer()

  implicit val ec = system.dispatcher

  implicit val awsLambdaClient = mock[AWSLambdaAsyncClient]

  override protected def afterEach(): Unit = {
    reset(awsLambdaClient)
    verifyNoMoreInteractions(awsLambdaClient)
  }

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  "AwsLambdaFlow" should {

    val invokeRequest = new InvokeRequest().withFunctionName("test_function").withPayload("test_payload")
    val invokeFailureRequest = new InvokeRequest().withFunctionName("failure_function").withPayload("test_payload")
    val invokeResult = new InvokeResult()
    val lambdaFlow = AwsLambdaFlow(1)

    "call a single invoke request" in {

      when(
        awsLambdaClient.invokeAsync(mockitoEq(invokeRequest), mockitoAny[AsyncHandler[InvokeRequest, InvokeResult]]())
      ).thenAnswer(new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): AnyRef = {
          invocation.getArgument[AsyncHandler[InvokeRequest, InvokeResult]](1).onSuccess(invokeRequest, invokeResult)
          CompletableFuture.completedFuture(invokeResult)
        }
      })

      val (probe, future) = TestSource.probe[InvokeRequest].via(lambdaFlow).toMat(Sink.seq)(Keep.both).run()
      probe.sendNext(invokeRequest)
      probe.sendComplete()

      Await.result(future, 3.seconds) shouldBe Vector(invokeResult)
      verify(awsLambdaClient, times(1)).invokeAsync(mockitoEq(invokeRequest),
                                                    mockitoAny[AsyncHandler[InvokeRequest, InvokeResult]]())

    }

    "call with exception" in {

      when(
        awsLambdaClient.invokeAsync(mockitoAny[InvokeRequest](),
                                    mockitoAny[AsyncHandler[InvokeRequest, InvokeResult]]())
      ).thenAnswer(new Answer[Future[InvokeResult]] {
        override def answer(invocation: InvocationOnMock): Future[InvokeResult] = {
          val exception = new RuntimeException("Error in lambda")
          invocation.getArgument[AsyncHandler[InvokeRequest, InvokeResult]](1).onError(exception)
          val future = new CompletableFuture[InvokeResult]()
          future.completeExceptionally(exception)
          future
        }
      })

      val (probe, future) = TestSource.probe[InvokeRequest].via(lambdaFlow).toMat(Sink.seq)(Keep.both).run()

      probe.sendNext(invokeFailureRequest)
      probe.sendComplete()

      Await.result(future.failed, 3.seconds) shouldBe a[RuntimeException]
    }

  }
}
