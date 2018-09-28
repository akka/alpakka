/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.e2e
import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.alpakka.google.cloud.bigquery.client.GoogleEndpoints
import akka.stream.alpakka.google.cloud.bigquery.scaladsl.GoogleBigQuerySource
import akka.stream.scaladsl.Sink

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

trait BigQueryTestHelper {
  import com.typesafe.config.ConfigFactory

  implicit val actorSystem: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit lazy val ec: ExecutionContext = actorSystem.dispatcher

  val defaultTimeout = 5.seconds

  lazy val config = ConfigFactory.load()

  lazy val enableE2E = config.getBoolean("bigquery.enable-e2e")

  lazy val projectId = config.getString("bigquery.dbconfig.projectId")
  lazy val dataset = config.getString("bigquery.dbconfig.dataset")
  lazy val clientEmail = config.getString("bigquery.dbconfig.clientEmail")
  lazy val privateKey = config.getString("bigquery.dbconfig.privateKey").replace("\\n", "\n")

  lazy val connection = GoogleBigQuerySource.createProjectConfig(
    clientEmail = clientEmail,
    privateKey = privateKey,
    projectId = projectId,
    dataset = dataset
  )

  def sleep(): Unit =
    Await.result(Future(Thread.sleep(1000)), 2.seconds)

  def await[T](f: Future[T]): T = Await.result(f, defaultTimeout)

  def runRequest(httpRequest: HttpRequest): Future[Done] =
    GoogleBigQuerySource.raw(httpRequest, x => x, connection.session).runWith(Sink.ignore)

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
