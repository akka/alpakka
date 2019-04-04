/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Inside, Matchers, WordSpecLike}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

trait BaseSpec
    extends WordSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with Inside
    with AkkaSupport
    with FtpSupport {

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed]

  protected def listFilesWithFilter(basePath: String, branchSelector: FtpFile => Boolean): Source[FtpFile, NotUsed]

  protected def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]]

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]]

  protected def remove(): Sink[FtpFile, Future[IOResult]]

  protected def move(destinationPath: FtpFile => String): Sink[FtpFile, Future[IOResult]]

  protected def startServer(): Unit

  protected def stopServer(): Unit

  /** For a few tests `assertAllStagesStopped` failed on Travis, this hook allows to inject a bit more patience
   * for the check.
   *
   * Can be removed after upgrade to Akka 2.5.22 https://github.com/akka/akka/issues/26410
   */
  protected def extraWaitForStageShutdown(): Unit = ()

  after {
    cleanFiles()
  }

  override protected def beforeAll() = {
    super.beforeAll()
    startServer()
  }

  override protected def afterAll() = {
    stopServer()
    Await.ready(getSystem.terminate(), 42.seconds)
    super.afterAll()
  }
}
