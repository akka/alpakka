/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
import io.specto.hoverfly.junit.core.{Hoverfly, HoverflyConfig, HoverflyMode, SimulationSource}

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
  lazy val privateKey = "-----BEGIN PRIVATE KEY-----\nMIIEowIBAAKCAQEAq8G78M3QSgZH6TWz3/HIzOgzfUxgEjto41iYoV76msgGubjR\nkbCUsWggUR9Us4yR13IeoR9Pbw7aG4RxJ+xms4kKW6v0ZyPqtgV7HDbW78ECtKM0\nzoYSQ/OF7eAz08o4KV7/YKMYLC8GIIsWyViBb1Bne806+1N7GMhN2xhCiq9bINcb\nD+qkZl/Gk/mnKb4e23UqwU+MC6tI6TXFXKQ49z8HH/09wwLzAS1bjrszfNuLtEBJ\nXRV5qEqZ7DTK4rUTGEO1zSj9WFujEKQpHUDyJDgcpYvMbsC91idcKBrqXLnxbfg/\nuoq9OwJi+bJVryawOrLUKL58/u+c4gkXonBt4wIDAQABAoIBAFAgi3slqRw/neCw\nSwAYniLp2MuFi/Q1fxNAy8PMuYDn/Cs8i5g6FsRE3X667RruY0NtW9iy8K3Q8fOQ\nb/G+GZN8RTbLG7PaT68nE23wL4meM5Lt6L7IUVEeFMcKp2MQne6/AMimjapfLa6U\n9MZt4cR6cCyTbAa/xVekap3hzXlA3bpW3FrXhoKdDpTLaUKYaeCggXdVsg0riK8c\n9Uts14Wz4ZehpJOhEDvoA3dL1yebA4XKBZcvMe4oKn9K/qgVHVKBnUcrbnuYcTB9\nqW5QF5/yKveKLnppPzr8jHov8B12Itag4ocmtTBf0f6YLASjDQtfEDbd/LJeh0jq\nGKddS5ECgYEA1661tNG1gllhcRDJvq2n800Ac0GSpCIlruCWzK21VJENo0U91pz+\n/mkXOPUhodgflDL9pPL4DHFtFd4f3o/qI8oX7earM9+NbHtw316bAHDOxIHupF0C\ngT7hbBkycJadBnOO+tzenz/2I3JhD3pEgksSVQYUzHHBqm9fw2QUiHcCgYEAy90A\nCPgGZk6tCac1WaflxP5ierYeudYEFl6D+RlJmUVbx94AunNRtZ67j1bWnzq2WO2L\noKuE7Qb0FZWhUvxVbmmghVPSW8Z70KhEZ0TBl71YARgzODUuNIRrM9/Y/+ZQXCBP\nb8T+qBP1M8Ts8TqzxzpMLsVNALZ7c53OEvANzPUCgYBJ4g1YvaXB20Bn7OpPKUmp\nLK2Ezeef1hq2hzThNHgzWeUkEuoWBH3NRM6xsjctK83VhIoi4SBbktddcFPWd9Ir\nJGWCF0x6XpAhoz+NJOlQA1SxOBk5sKrU/2dVEmSW8OElfpxyDwsr3ktA5UOee7HQ\nOEs1WPny9tzyt2hElJn8DQKBgBG4r2UYMm44TqB1MZUOnFGoj2T9aeRbr1VGeBBy\nW0yAk/7m1IdguOyh1MocEWIcF3fZhna8Ej0MirFJpZFyL/b+JZ8Rb0rdESxNREz5\n1B5drkXCFcnADbkw/aSvw8xS+A9aG62qoTx5J6qNZs99e91IuxChxBTYyBh/0kch\nKQH1AoGBANRbcrLyF41TYgpOA7z3xLcnL+h2AS14syk4PTPFzuukNvbYprlBq4Ls\nphLss+UJwK5L6VB4EJVkjZdbQ1c+AX9IPft62f2oDbFK9C8x2VG0b6T2i/zNDfu1\nRo4OWAxRttntrc/qfimdvgVtvCSFsCpkqaj9lCTyJqgbYh+aUCdx\n-----END PRIVATE KEY-----\\n".replace("\\n", "\n")

  val certPath = getClass.getClassLoader.getResource("cert.pem").getPath
  val forwardProxy = ForwardProxy(proxyHost,proxyPort, Option.empty, Option(ForwardProxyTrustPem(certPath)))

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
