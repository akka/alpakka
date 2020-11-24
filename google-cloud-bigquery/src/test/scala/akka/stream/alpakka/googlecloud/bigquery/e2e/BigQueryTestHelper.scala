/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e
import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryConfig, ForwardProxy, ForwardProxyTrustPem}
import akka.stream.alpakka.googlecloud.bigquery.client.GoogleEndpoints
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQueryCallbacks, GoogleBigQuerySource}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.SprayJsonSupport._
import akka.stream.scaladsl.Sink
import spray.json.JsValue

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

trait BigQueryTestHelper {
  import com.typesafe.config.ConfigFactory

  implicit val actorSystem: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit lazy val ec: ExecutionContext = actorSystem.dispatcher

  val defaultTimeout = 5.seconds

  lazy val config = ConfigFactory.load()

  val proxyHost = "localhost"
  val proxyPort = 8500

  lazy val projectId = "bigqueryproject"
  lazy val dataset = "bigquerydataset"
  lazy val clientEmail = "big-query-sa@bigqueryproject.iam.gserviceaccount.com"

  lazy val privateKey = {
    val inputStream = getClass.getClassLoader.getResourceAsStream("private_pcks8.pem")
    Source.fromInputStream(inputStream).getLines().mkString("\n").stripMargin
  }

  val certPath = getClass.getClassLoader.getResource("cert.pem").getPath
  val forwardProxy = ForwardProxy(proxyHost, proxyPort, Option.empty, Option(ForwardProxyTrustPem(certPath)))

  lazy val projectConfig = BigQueryConfig(
    clientEmail = clientEmail,
    privateKey = privateKey,
    projectId = projectId,
    dataset = dataset,
    forwardProxy
  )

  def sleep(): Unit =
    Await.result(Future(Thread.sleep(1000)), 2.seconds)

  def await[T](f: Future[T]): T = Await.result(f, defaultTimeout)

  def runRequest(httpRequest: HttpRequest): Future[Done] = {
    implicit val unmarshaller = Unmarshaller.strict((x: JsValue) => x)
    GoogleBigQuerySource
      .raw(httpRequest, BigQueryCallbacks.ignore, projectConfig)
      .runWith(Sink.ignore)
  }

  def createTable(schemaDefinition: String): HttpRequest = HttpRequest(
    HttpMethods.POST,
    Uri(GoogleEndpoints.createTableUrl(projectId, dataset)),
    entity = HttpEntity(ContentTypes.`application/json`, schemaDefinition)
  )

  def insertInto(data: String, table: String) = HttpRequest(
    HttpMethods.POST,
    Uri(GoogleEndpoints.insertIntoUrl(projectId, dataset, table)),
    entity = HttpEntity(ContentTypes.`application/json`, data)
  )

  def dropTable(table: String) =
    HttpRequest(HttpMethods.DELETE, Uri(GoogleEndpoints.deleteTableUrl(projectId, dataset, table)))
}
