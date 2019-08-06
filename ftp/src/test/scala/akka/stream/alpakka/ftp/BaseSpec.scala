/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ftp

import akka.{Done, NotUsed}
import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{
  Args,
  BeforeAndAfter,
  BeforeAndAfterAll,
  Inside,
  Matchers,
  Status,
  TestSuite,
  TestSuiteMixin,
  WordSpecLike
}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

trait BaseSpec
    extends TestSuiteMixin
    with WordSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with Inside
    with AkkaSupport
    with BaseSupport { this: TestSuite =>

  protected def listFiles(basePath: String): Source[FtpFile, NotUsed]

  protected def listFilesWithFilter(basePath: String,
                                    branchSelector: FtpFile => Boolean,
                                    emitTraversedDirectories: Boolean = false): Source[FtpFile, NotUsed]

  protected def retrieveFromPath(path: String, fromRoot: Boolean = false): Source[ByteString, Future[IOResult]]

  protected def retrieveFromPathWithOffset(path: String, offset: Long): Source[ByteString, Future[IOResult]]

  protected def storeToPath(path: String, append: Boolean): Sink[ByteString, Future[IOResult]]

  protected def remove(): Sink[FtpFile, Future[IOResult]]

  protected def move(destinationPath: FtpFile => String): Sink[FtpFile, Future[IOResult]]

  protected def mkdir(basePath: String, name: String): Source[Done, NotUsed]

  /** For a few tests `assertAllStagesStopped` failed on Travis, this hook allows to inject a bit more patience
   * for the check.
   *
   * Can be removed after upgrade to Akka 2.5.22 https://github.com/akka/akka/issues/26410
   */
  protected def extraWaitForStageShutdown(): Unit = ()

  after {
    cleanFiles()
  }

  override protected def afterAll() = {
    Await.ready(getSystem.terminate(), 42.seconds)
    super.afterAll()
  }

  // Allows to run tests n times in a row with a command line argument, useful for debugging sporadic failures
  // e.g. ftp/testOnly *.FtpsStageSpec -- -Dtimes=20
  // https://gist.github.com/dwickern/6ba9c5c505d2325d3737ace059302922
  protected abstract override def runTest(testName: String, args: Args): Status = {
    def run0(times: Int): Status = {
      val status = super.runTest(testName, args)
      if (times <= 1) status else status.thenRun(run0(times - 1))
    }
    run0(args.configMap.getWithDefault("times", "1").toInt)
  }
}
