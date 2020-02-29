/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e
import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryConfig, ForwardProxy, ForwardProxyTrustPem}
import akka.stream.alpakka.googlecloud.bigquery.client.GoogleEndpoints
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQueryCallbacks, GoogleBigQuerySource}
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

  val proxyHost = "localhost"
  val proxyPort = 8500

  lazy val projectId = "bigqueryproject"
  lazy val dataset = "bigquerydataset"
  lazy val clientEmail = "big-query-sa@bigqueryproject.iam.gserviceaccount.com"

  // openssl genrsa -out mykey.pem 1024
  // openssl pkcs8 -topk8 -nocrypt -in mykey.pem -out myrsakey_pcks8
  // openssl rsa -in mykey.pem -pubout > mykey.pub
  lazy val privateKey =
    """-----BEGIN PRIVATE KEY-----
      |MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAMwkmdwrWp+LLlsf
      |bVE+neFjZtUNuaD4/tpQ2UIh2u+qU6sr4bG8PPuqSdrt5b0/0vfMZA11mQWmKpg5
      |PK98kEkhbSvC08fG0TtpR9+vflghOuuvcw6kCniwNbHlOXnE8DwtKQp1DbTUPzMD
      |hhsIjJaUtv19Xk7gh4MqYgANTm6lAgMBAAECgYEAwBXIeHSKxwiNS8ycbg//Oq7v
      |eZV6j077bq0YYLO+cDjSlYOq0DSRJTSsXcXvoE1H00aM9mUq4TfjaGyi/3SzxYsr
      |rSzu/qpYC58MJsnprIjlLgFZmZGe5MOSoul/u6JsBTJGkYPV0xGrtXJY103aSYzC
      |xthpY0BHy9eO9I/pNlkCQQD/64g4INAiBdM4R5iONQvh8LLvqbb8Bw4vVwVFFnAr
      |YHcomxtT9TunMad6KPgbOCd/fTttDADrv54htBrFGXeXAkEAzDTtisPKXPByJnUd
      |jKO2oOg0Fs9IjGeWbnkrsN9j0134ldARE+WbT5S8G5EFo+bQi4ffU3+Y/4ly6Amm
      |OAAzIwJBANV2GAD5HaHDShK/ZTf4dxjWM+pDnSVKnUJPS039EUKdC8cK2RiGjGNA
      |v3jdg1Tw2cE1K8QhJwN8qOFj4JBWVbECQQCwcntej9bnf4vi1wd1YnCHkJyRqQIS
      |7974DhNGfYAQPv5w1JwtCRSuKuJvH1w0R1ijd//scjCNfQKgpNXPRbzpAkAQ8MFA
      |MLpOLGqezUQthJWmVtnXEXaAlb3yFSRTZQVEselObiIc6EvYzNXv780IDT4pyKjg
      |8DS9i5jJDIVWr7mA
      |-----END PRIVATE KEY-----
  """.stripMargin

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

  def runRequest(httpRequest: HttpRequest): Future[Done] =
    GoogleBigQuerySource
      .raw(httpRequest, x => Option(x), BigQueryCallbacks.ignore, projectConfig)
      .runWith(Sink.ignore)

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
