/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.integration

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCode
import akka.stream.alpakka.typesense.TypesenseSettings
import akka.stream.alpakka.typesense.impl.TypesenseHttp.TypesenseException
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import java.util.concurrent.CompletionStage
import scala.concurrent.Future
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success}

/**
 * Integration tests for Typesense using Docker container.
 * The idea is to execute tests for two Typesense versions:
 * the newest one (0.23.0) and the oldest with available documentation (0.11.2)
 */
abstract class TypesenseIntegrationSpec(protected val version: String)
    extends AnyFunSpec
    with TestContainerForAll
    with ScalaFutures
    with should.Matchers {
  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  private val versionFileSufix = version.replace('.', '_')
  private val dockerComposeFile: File = new File(s"typesense/src/test/resources/docker-compose-$versionFileSufix.yml")
  private val port = 8108
  private val containerName = "typesense"
  private val exposedService: ExposedService =
    ExposedService(containerName, port, Wait.forHttp("/collections").forStatusCode(401))
  private val apiKey = "Hu52dwsas2AdxdE"

  protected val settings: TypesenseSettings = TypesenseSettings(s"http://localhost:$port", apiKey)

  override val containerDef: DockerComposeContainer.Def =
    DockerComposeContainer.Def(dockerComposeFile, Seq(exposedService))

  protected val JavaTypesense = akka.stream.alpakka.typesense.javadsl.Typesense

  protected def runWithFlow[Request, Response](request: Request,
                                               flow: Flow[Request, Response, Future[NotUsed]]): Response =
    Source
      .single(request)
      .via(flow)
      .runWith(Sink.head)
      .futureValue

  protected def runWithSink[Request](request: Request, sink: Sink[Request, Future[Done]]): Done =
    Source
      .single(request)
      .toMat(sink)(Keep.right)
      .run()
      .futureValue

  protected def runWithJavaFlow[Request, Response](
      request: Request,
      flow: akka.stream.javadsl.Flow[Request, Response, CompletionStage[NotUsed]]
  ): Response =
    Source
      .single(request)
      .via(flow)
      .runWith(Sink.head)
      .futureValue

  protected def runWithJavaSink[Request](request: Request,
                                         sink: akka.stream.javadsl.Sink[Request, CompletionStage[Done]]): Done =
    Source
      .single(request)
      .toMat(sink)(Keep.right)
      .run()
      .asScala
      .futureValue

  protected def tryUsingFlowAndExpectError[Request](request: Request,
                                                    flow: Flow[Request, _, Future[NotUsed]],
                                                    expectedStatusCode: StatusCode): Assertion = {
    val response = Source
      .single(request)
      .via(flow)
      .map(Success.apply)
      .recover(e => Failure(e))
      .runWith(Sink.head)
      .futureValue

    val gotStatusCode = response.toEither.swap.toOption.get.asInstanceOf[TypesenseException].statusCode

    gotStatusCode shouldBe expectedStatusCode
  }

  protected def tryUsingSinkAndExpectError[Request](request: Request,
                                                    sink: Sink[Request, Future[Done]],
                                                    expectedStatusCode: StatusCode): Assertion = {

    val result = Source
      .single(request)
      .toMat(sink)(Keep.right)
      .run()
      .map(Success.apply)
      .recover(e => Failure(e))
      .futureValue

    val gotStatusCode = result.toEither.swap.toOption.get.asInstanceOf[TypesenseException].statusCode
    gotStatusCode shouldBe expectedStatusCode
  }

  protected def tryUsingDirectRequestAndExpectError(future: Future[_], expectedStatusCode: StatusCode): Assertion = {
    val result = future
      .map(Success.apply)
      .recover(e => Failure(e))
      .futureValue

    val gotStatusCode = result.toEither.swap.toOption.get.asInstanceOf[TypesenseException].statusCode
    gotStatusCode shouldBe expectedStatusCode
  }

  protected def tryUsingJavaFlowAndExpectError[Request](
      request: Request,
      flow: akka.stream.javadsl.Flow[Request, _, CompletionStage[NotUsed]],
      expectedStatusCode: StatusCode
  ): Assertion = {
    val response = Source
      .single(request)
      .via(flow)
      .map(Success.apply)
      .recover(e => Failure(e))
      .runWith(Sink.head)
      .futureValue

    val gotStatusCode = response.toEither.swap.toOption.get.asInstanceOf[TypesenseException].statusCode

    gotStatusCode shouldBe expectedStatusCode
  }

  protected def tryUsingJavaSinkAndExpectError[Request](request: Request,
                                                        sink: akka.stream.javadsl.Sink[Request, CompletionStage[Done]],
                                                        expectedStatusCode: StatusCode): Assertion = {

    val result = Source
      .single(request)
      .toMat(sink)(Keep.right)
      .run()
      .asScala
      .map(Success.apply)
      .recover(e => Failure(e))
      .futureValue

    val gotStatusCode = result.toEither.swap.toOption.get.asInstanceOf[TypesenseException].statusCode
    gotStatusCode shouldBe expectedStatusCode
  }

  protected def tryUsingJavaDirectRequestAndExpectError(completionStage: CompletionStage[_],
                                                        expectedStatusCode: StatusCode): Assertion = {
    val result = completionStage.asScala
      .map(Success.apply)
      .recover(e => Failure(e))
      .futureValue

    val gotStatusCode = result.toEither.swap.toOption.get.asInstanceOf[TypesenseException].statusCode
    gotStatusCode shouldBe expectedStatusCode
  }
}
