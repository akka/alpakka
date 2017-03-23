/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ftp

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Inside, Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

trait BaseSpec
    extends WordSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with ScalaFutures
    with Inside
    with AkkaSupport
    with FtpSupport {

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed]

  protected def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]]

  protected def startServer(): Unit

  protected def stopServer(): Unit

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
